
#include <result_processor.h>
#include <dep/rmr/rmr.h>
#include <search_cluster.h>
#include <query_plan.h>
#include <commands.h>
#include <aggregate/aggregate.h>
#include <util/arr.h>
#include "dist_plan.h"
#include <err.h>

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

/* Get cursor command using a cursor id and an existing aggregate command */
static int getCursorCommand(MRReply *prev, MRCommand *cmd) {
  long long cursorId;
  if (!MRReply_ToInteger(MRReply_ArrayElement(prev, 1), &cursorId)) {
    // Invalid format?!
    return 0;
  }
  if (cursorId == 0) {
    // Cursor was set to 0, end of reply chain.
    return 0;
  }
  if (cmd->num < 2) {
    return 0;  // Invalid command!??
  }

  char buf[128];
  sprintf(buf, "%lld", cursorId);
  const char *idx = cmd->args[1];
  MRCommand newCmd = MR_NewCommand(4, "_" RS_CURSOR_CMD, "READ", idx, buf);
  MRCommand_Free(cmd);
  *cmd = newCmd;
  return 1;
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

  // rewrite and resend the cursor command if needed
  int rc = REDIS_OK;
  int isDone = !getCursorCommand(rep, cmd);

  // Push the reply down the chain
  MRReply *arr = MRReply_ArrayElement(rep, 0);
  if (arr && MRReply_Type(arr) == MR_REPLY_ARRAY && MRReply_Length(arr) > 1) {
    MRIteratorCallback_AddReply(ctx, rep);
    // User code now owns the reply, so we can't free it here ourselves!
    rep = NULL;
  } else {
    isDone = 1;
  }

  if (isDone) {
    MRIteratorCallback_Done(ctx, 0);
  } else {
    // resend command
    if (REDIS_ERR == MRIteratorCallback_ResendCommand(ctx, cmd)) {
      MRIteratorCallback_Done(ctx, 1);
      rc = REDIS_ERR;
    }
  }

  if (rep != NULL) {
    // If rep has been set to NULL, it means the callback has been invoked
    MRReply_Free(rep);
  }
  return rc;
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
  struct {
    MRReply *root;  // Root reply. We need to free this when done with the rows
    MRReply *rows;  // Array containing reply rows for quick access
  } current;

  MRReply *currentRoot;
  size_t curIdx;
  size_t numReplies;
  uint64_t totalCount;
  AggregateSchema schema;
  MRIterator *it;
  MRCommand cmd;
  MRCommandGenerator cg;
};

static int getNextReply(struct netCtx *nc) {
  while (1) {
    MRReply *root = MRIterator_Next(nc->it);
    if (root == MRITERATOR_DONE) {
      // No more replies
      nc->current.root = NULL;
      nc->current.rows = NULL;
      return 0;
    }

    MRReply *rows = MRReply_ArrayElement(root, 0);
    if (rows == NULL || MRReply_Type(rows) != MR_REPLY_ARRAY || MRReply_Length(rows) == 0) {
      MRReply_Free(root);
      continue;
    }
    nc->current.root = root;
    nc->current.rows = rows;
    return 1;
  }
}

static int net_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct netCtx *nc = ctx->privdata;
  // if we've consumed the last reply - free it
  if (nc->current.rows && nc->curIdx == MRReply_Length(nc->current.rows)) {
    MRReply_Free(nc->current.root);
    nc->current.root = nc->current.rows = NULL;
  }

  // get the next reply from the channel
  if (!nc->current.root) {
    if (!getNextReply(nc)) {
      return RS_RESULT_EOF;
    }
    int totalIdx = 0;
    // the first reply returns the schema as the first element, the others the number of results
    if (MRReply_Type(MRReply_ArrayElement(nc->current.rows, 0)) ==
        MR_REPLY_ARRAY) {  // <--- this is a schema response
                           // the total will be at the second element in this case
      totalIdx++;
      if (!nc->schema) {
        // This seems to work
        nc->schema = net_extractSchema(MRReply_ArrayElement(nc->current.rows, 0));
      }  // Otherwise another node probably gave us a schema already.

      ctx->qxc->totalResults += MRReply_Integer(MRReply_ArrayElement(nc->current.rows, totalIdx));
    }
    nc->curIdx = totalIdx + 1;
  }

  MRReply *rep = MRReply_ArrayElement(nc->current.rows, nc->curIdx++);

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

  if (nc->current.root) {
    MRReply_Free(nc->current.root);
  }

  if (nc->it) MRIterator_Free(nc->it);
  free(nc);
  free(rp);
}
ResultProcessor *NewNetworkFetcher(RedisSearchCtx *sctx, MRCommand cmd, SearchCluster *sc) {

  //  MRCommand_FPrint(stderr, &cmd);
  struct netCtx *nc = malloc(sizeof(*nc));
  nc->curIdx = 0;
  nc->current.root = NULL;
  nc->current.rows = NULL;
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

static ResultProcessor *buildDistributedProcessorChain(QueryPlan *plan, void *ctx, char **err) {
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

  // AggregatePlan_Print(remote);
  // AggregatePlan_Print(ap);

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

void AggregateCommand_ExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                        struct ConcurrentCmdCtx *ccx) {

  AggregateRequestSettings settings = {.pcb = buildDistributedProcessorChain,
                                       .flags = AGGREGATE_REQUEST_NO_CONCURRENT |
                                                AGGREGATE_REQUEST_NO_PARSE_QUERY |
                                                AGGREGATE_REQUEST_SPECLESS};
  settings.cursorLookupName = RedisModule_StringPtrLen(argv[1], NULL);
  AggregateCommand_ExecAggregateEx(ctx, argv, argc, ccx, &settings);
}
