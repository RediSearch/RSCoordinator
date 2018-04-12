
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

int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {
  // printf("Hello! Got cursor reply!!!!\n");
  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 2) {
    // printf("We think the reply is done (r=%p), (t=%d)!\n", rep, MRReply_Type(rep));
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
    // try to resend
    if (!isDone) {
      if (REDIS_ERR == MRIteratorCallback_ResendCommand(ctx, cmd)) {
        // MRReply_Free(rep);

        MRIteratorCallback_Done(ctx, 1);
        return REDIS_ERR;
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
  }
  /// MRReply_Free(rep);

  return REDIS_OK;
}

RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RS_NullVal();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      char *s = MRReply_String(r, &l);
      v = RS_StringValT(s, l, RSString_Volatile);
      break;
    }
    case MR_REPLY_INTEGER:
      v = RS_NumVal((double)MRReply_Integer(r));
      break;
    case MR_REPLY_ARRAY: {
      RSValue **arr = calloc(MRReply_Length(r), sizeof(*arr));
      for (size_t i = 0; i < MRReply_Length(r); i++) {
        arr[i] = RSValue_IncrRef(MRReply_ToValue(MRReply_ArrayElement(r, i)));
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
  MRIterator *it;
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
      // printf("Starting parent iterator..\n");
      nc->current = MRIterator_Next(nc->it);
      // printf("Got new current: %p\n", nc->current);
      if (nc->current == MRITERATOR_DONE) {
        nc->current = NULL;
        return RS_RESULT_EOF;
      }
    } while (!nc->current || MRReply_Type(nc->current) != MR_REPLY_ARRAY);

    nc->curIdx = 1;
  }

  MRReply *rep = MRReply_ArrayElement(nc->current, nc->curIdx++);

  if (r->fields) {
    RSFieldMap_Reset(r->fields);
  } else {
    r->fields = RS_NewFieldMap(8);
  }

  for (int i = 0; i < MRReply_Length(rep); i += 2) {
    // MRReply_Print(stderr, rep);
    const char *c = MRReply_String(MRReply_ArrayElement(rep, i), NULL);
    RSValue *v = MRReply_ToValue(MRReply_ArrayElement(rep, i + 1));

    RSFieldMap_Add(&r->fields, c, v);
  }
  return RS_RESULT_OK;
}

void net_Free(ResultProcessor *rp) {
  struct netCtx *nc = rp->ctx.privdata;

  nc->cg.Free(nc->cg.ctx);

  if (nc->current) {
    MRReply_Free(nc->current);
  }
  // free(nc->replies);
  if (nc->it) MRIterator_Free(nc->it);
  free(nc);
  free(rp);
}
ResultProcessor *NewNetworkFetcher(RedisSearchCtx *sctx, MRCommand cmd, SearchCluster *sc) {

  MRCommand_FPrint(stderr, &cmd);
  struct netCtx *nc = malloc(sizeof(*nc));
  nc->curIdx = 0;
  nc->current = NULL;
  nc->numReplies = 0;
  nc->it = NULL;
  nc->cg = SearchCluster_MultiplexCommand(sc, &cmd);

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
