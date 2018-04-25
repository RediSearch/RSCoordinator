
#include <result_processor.h>
#include <dep/rmr/rmr.h>
#include <search_cluster.h>
#include <query_plan.h>
#include <commands.h>
#include <aggregate/aggregate.h>
#include <util/arr.h>
#include "dist_plan.h"
#include <err.h>

/* Get cursor command using a cursor id and an existing aggregate command */
int getCursorCommand(MRCommand *cmd, long long cursorId) {
  if (cmd->num < 2 || !cursorId) return 0;

  char buf[128];
  sprintf(buf, "%lld", cursorId);
  const char *idx = cmd->args[1];
  MRCommand newCmd = MR_NewCommand(4, "_" RS_CURSOR_CMD, "READ", idx, buf);
  MRCommand_Free(cmd);
  *cmd = newCmd;
  return 1;
}

/* Exctract the schema from WITHSCHEMA */
AggregateSchema net_extractSchema(MRReply *rep) {
  if (rep == NULL || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) == 0) {
    return NULL;
  }

  AggregateSchema sc = array_new(AggregateProperty, MRReply_Length(rep));
  for (size_t i = 0; i < MRReply_Length(rep); i++) {
    MRReply *e = MRReply_ArrayElement(rep, i);
    if (e && MRReply_Type(e) == MR_REPLY_ARRAY && MRReply_Length(e) == 2) {
      RSValueType t = RSValue_String;
      size_t nlen;
      const char *name = MRReply_String(MRReply_ArrayElement(e, 0), &nlen);
      if (MRReply_StringEquals(MRReply_ArrayElement(e, 1), "number", 0)) {
        t = RSValue_Number;
      } else if (MRReply_StringEquals(MRReply_ArrayElement(e, 1), "array", 0)) {
        t = RSValue_Array;
      }
      sc = array_append(sc, ((AggregateProperty){
                                .property = strndup(name, nlen),
                                .kind = Property_Field,
                                .type = t,
                            }));
    }
    // fprintf(stderr, "%s %s\n", array_tail(sc).property, RSValue_TypeName(array_tail(sc).type));
  }

  return sc;
}

int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {
  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 2) {
    if (MRReply_Type(rep) == MR_REPLY_ERROR) {
      // printf("Error is '%s'\n", MRReply_String(rep, NULL));
    }
    MRReply_Free(rep);
    MRIteratorCallback_Done(ctx, 1);
    return REDIS_ERR;
  }
  // printf("Continuing processing...\n");

  // rewrite and resend the cursor command if needed
  int isDone = 0;
  long long curs = 0;

  if (MRReply_ToInteger(MRReply_ArrayElement(rep, 1), &curs) && curs > 0) {
    if (strcasecmp(cmd->args[0], "_" RS_CURSOR_CMD)) {
      if (!getCursorCommand(cmd, curs)) {
        isDone = 1;
      }
    }

  } else {
    isDone = 1;
  }

  // Push the reply down the chain
  MRReply *arr = MRReply_ArrayElement(rep, 0);
  if (arr && MRReply_Type(arr) == MR_REPLY_ARRAY && MRReply_Length(arr) > 1) {
    MRIteratorCallback_AddReply(ctx, arr);
  } else {
    // MRReply_Free(arr);
    isDone = 1;
  }

  if (isDone) {
    MRIteratorCallback_Done(ctx, 0);
  } else {
    // resend command
    if (REDIS_ERR == MRIteratorCallback_ResendCommand(ctx, cmd)) {
      // MRReply_Free(rep);

      MRIteratorCallback_Done(ctx, 1);
      return REDIS_ERR;
    }
  }
  /// MRReply_Free(rep);

  return REDIS_OK;
}

RSValue *MRReply_ToValue(MRReply *r, RSValueType convertType) {
  if (!r) return RS_NullVal();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      char *s = MRReply_String(r, &l);
      // If we need to convert the value to number - do it now...
      if (convertType == RSValue_Number) {
        v = RSValue_ParseNumber(s, l);

        if (!v) v = RS_NullVal();
      } else {
        v = RS_StringValT(s, l, RSString_Volatile);
      }
      break;
    }
    case MR_REPLY_INTEGER:
      v = RS_NumVal((double)MRReply_Integer(r));
      break;
    case MR_REPLY_ARRAY: {
      RSValue **arr = calloc(MRReply_Length(r), sizeof(*arr));
      for (size_t i = 0; i < MRReply_Length(r); i++) {
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i), RSValue_String);
      }
      v = RS_ArrVal(arr, MRReply_Length(r));
      break;
    }
    case MR_REPLY_NIL:
    default:
      v = RS_NullVal();
      break;
  }
  return v;
}

struct netCtx {
  // MRReply **replies;
  MRReply *current;
  size_t curIdx;
  size_t numReplies;
  uint64_t totalCount;
  AggregateSchema schema;
  MRIterator *it;
  MRCommand cmd;
  MRCommandGenerator cg;
};

static int net_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  // printf("Hello!\n");
  struct netCtx *nc = ctx->privdata;
  // if we've consumed the last reply - free it
  if (nc->current && nc->curIdx == MRReply_Length(nc->current)) {
    MRReply_Free(nc->current);
    nc->current = NULL;
  }

  // get the next reply from the channel
  if (!nc->current) {
    do {
      nc->current = MRIterator_Next(nc->it);
      if (nc->current == MRITERATOR_DONE) {
        nc->current = NULL;

        return RS_RESULT_EOF;
      }
    } while (!nc->current || MRReply_Type(nc->current) != MR_REPLY_ARRAY ||
             MRReply_Length(nc->current) == 0);

    int totalIdx = 0;
    if (nc->current) {
      // the first reply returns the schema as the first element, the others the number of results
      if (MRReply_Type(MRReply_ArrayElement(nc->current, 0)) ==
          MR_REPLY_ARRAY) {  // <--- this is a schema response
                             // the total will be at the second element in this case
        totalIdx++;
        if (!nc->schema) {
          nc->schema = net_extractSchema(MRReply_ArrayElement(nc->current, 0));
        }
      }
      ctx->qxc->totalResults += MRReply_Integer(MRReply_ArrayElement(nc->current, totalIdx));
    }
    nc->curIdx = totalIdx + 1;
  }

  MRReply *rep = MRReply_ArrayElement(nc->current, nc->curIdx++);

  if (r->fields) {
    RSFieldMap_Reset(r->fields);
  } else {
    r->fields = RS_NewFieldMap(8);
  }

  for (int i = 0; i < MRReply_Length(rep); i += 2) {
    const char *c = MRReply_String(MRReply_ArrayElement(rep, i), NULL);

    RSValueType t = RSValue_String;  // *p = NULL;
    MRReply *val = MRReply_ArrayElement(rep, i + 1);

    // If this is a string it might actually be a number, we need to extract the type from the
    // schema and convert to a number
    if (MRReply_Type(val) == MR_REPLY_STRING) {
      AggregateProperty *p = AggregateSchema_Get(nc->schema, c);
      if (p) t = p->type;
    }
    //}
    RSValue *v = MRReply_ToValue(val, t);

    RSFieldMap_Add(&r->fields, c, v);
  }
  return RS_RESULT_OK;
}

void net_Free(ResultProcessor *rp) {
  struct netCtx *nc = rp->ctx.privdata;

  // the iterator might not be done - some producers might still be sending data, let's wait for
  // them...
  MRIterator_WaitDone(nc->it);

  nc->cg.Free(nc->cg.ctx);

  if (nc->schema) {
    array_free_ex(nc->schema, free((char *)((AggregateProperty *)ptr)->property));
  }

  if (nc->current) {
    MRReply_Free(nc->current);
  }

  // free(nc->replies);
  if (nc->it) MRIterator_Free(nc->it);
  free(nc);
  free(rp);
}
ResultProcessor *NewNetworkFetcher(RedisSearchCtx *sctx, MRCommand cmd, SearchCluster *sc) {

  //  MRCommand_FPrint(stderr, &cmd);
  struct netCtx *nc = malloc(sizeof(*nc));
  nc->curIdx = 0;
  nc->current = NULL;
  nc->numReplies = 0;
  nc->it = NULL;
  nc->cmd = cmd;
  nc->totalCount = 0;
  nc->schema = NULL;
  nc->cg = SearchCluster_MultiplexCommand(sc, &nc->cmd);

  ResultProcessor *proc = NewResultProcessor(NULL, nc);
  proc->Free = net_Free;
  proc->Next = net_Next;
  return proc;
}

int NetworkFetcher_Start(struct netCtx *nc) {
  MRIterator *it = MR_Iterate(nc->cg, netCursorCallback, NULL);
  if (!it) {
    return 0;
  }
  nc->it = it;
  return 1;
}
int getAggregateFields(FieldList *l, RedisModuleCtx *ctx, CmdArg *cmd);

ResultProcessor *AggregatePlan_BuildProcessorChain(AggregatePlan *plan, RedisSearchCtx *sctx,
                                                   ResultProcessor *root, char **err);

static ResultProcessor *Aggregate_BuildDistributedChain(QueryPlan *plan, void *ctx, char **err) {

  AggregatePlan *ap = ctx;
  AggregatePlan *remote = AggregatePlan_MakeDistributed(ap);
  if (!remote) {
    SET_ERR(err, "Could not process plan for distribution");
    return NULL;
  }

  // Set hook to write the extracted schema if needed
  if (ap->withSchema) {
    AggregateSchema sc = AggregatePlan_GetSchema(ap, NULL);
    if (sc) {
      QueryPlan_SetHook(plan, QueryPlanHook_Pre, AggregatePlan_DumpSchema, sc, array_free);
    }
  }

  AggregatePlan_Print(remote);
  AggregatePlan_Print(ap);

  char **args = AggregatePlan_Serialize(remote);
  MRCommand xcmd = MR_NewCommandArgv(array_len(args), args);
  MRCommand_SetPrefix(&xcmd, "_FT");
  array_free_ex(args, free(*(void **)ptr));

  ResultProcessor *root = NewNetworkFetcher(plan->ctx, xcmd, GetSearchCluster());
  root->ctx.qxc = &plan->execCtx;
  ResultProcessor *ret = AggregatePlan_BuildProcessorChain(ap, plan->ctx, root, err);
  // err is null if there was a problem building the chain.
  // If it's not NULL we need to start the network fetcher now
  if (ret) {
    NetworkFetcher_Start(root->ctx.privdata);
  }
  return ret;
}

CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err);
int AggregateRequest_BuildDistributedPlan(AggregateRequest *req, RedisSearchCtx *sctx,
                                          RedisModuleString **argv, int argc, char **err) {

  req->args = Aggregate_ParseRequest(argv, argc, (char **)err);
  if (!req->args) {
    SET_ERR(err, "Could not parse aggregate request");
    return REDISMODULE_ERR;
  }

  req->ap = (AggregatePlan){};
  if (!AggregatePlan_Build(&req->ap, req->args, (char **)err)) {
    SET_ERR(err, "Could not parse aggregate request");
    return REDISMODULE_ERR;
  }

  RSSearchOptions opts = RS_DEFAULT_SEARCHOPTS;
  // mark the query as an aggregation query
  opts.flags |= Search_AggregationQuery;
  opts.concurrentMode = 0;
  req->plan =
      Query_BuildPlan(sctx, NULL, &opts, Aggregate_BuildDistributedChain, &req->ap, (char **)err);
  if (!req->plan) {
    SET_ERR(err, QUERY_ERROR_INTERNAL_STR);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
