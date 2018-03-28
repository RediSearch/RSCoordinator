
#include <result_processor.h>
#include <dep/rmr/rmr.h>
#include <search_cluster.h>
#include <query_plan.h>
#include <commands.h>
#include <aggregate/aggregate.h>

#define RESULTS_PER_ITERATION "350"

/* Get cursor command using a cursor id and an existing aggregate command */
int getCursorCommand(MRCommand *cmd, long long cursorId) {
  if (cmd->num < 2 || !cursorId) return 0;

  char buf[128];
  sprintf(buf, "%lld", cursorId);
  const char *idx = cmd->args[1];
  MRCommand newCmd =
      MR_NewCommand(6, "_" RS_CURSOR_CMD, "READ", idx, buf, "COUNT", RESULTS_PER_ITERATION);
  MRCommand_Free(cmd);
  *cmd = newCmd;
  return 1;
}

int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {

  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 2) {
    MRIteratorCallback_Done(ctx, 1);
    return REDIS_ERR;
  }

  // rewrite and resend the cursor command if needed
  int isDone = 0;
  long long curs = 0;
  if (MRReply_ToInteger(MRReply_ArrayElement(rep, 1), &curs) && curs > 0) {
    if (strcasecmp(cmd->args[0], "_" RS_CURSOR_CMD)) {
      if (!getCursorCommand(cmd, curs)) {
        isDone = 1;
      }
    }
    if (!isDone) MRIteratorCallback_ResendCommand(ctx, cmd);
  } else {
    isDone = 1;
  }

  // Push the reply down the chain
  MRReply *arr = MRReply_ArrayElement(rep, 0);
  if (arr && MRReply_Type(arr) == MR_REPLY_ARRAY && MRReply_Length(arr) > 1) {
    MRIteratorCallback_AddReply(ctx, arr);
  } else {
    isDone = 1;
  }
  if (isDone) {
    MRIteratorCallback_Done(ctx, 0);
  }

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
        arr[i] = MRReply_ToValue(MRReply_ArrayElement(r, i));
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
  MRReply *current;
  size_t curIdx;
  MRIterator *it;
};

static int net_Next(ResultProcessorCtx *ctx, SearchResult *r) {

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
    } while (!nc->current || MRReply_Type(nc->current) != MR_REPLY_ARRAY);

    nc->curIdx = 1;
  }

  MRReply *rep = MRReply_StealArrayElement(nc->current, nc->curIdx++);

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
  if (nc->current) {
    MRReply_Free(nc->current);
  }
  free(nc);
  // MRIterator *it = rp->ctx.privdata;
  // TODO: FREE
  // MRIterator_Free(it);
  free(rp);
}
ResultProcessor *NewNetworkFetcher(RedisSearchCtx *sctx, MRCommand cmd, SearchCluster *sc) {

  MRCommand_FPrint(stderr, &cmd);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(sc, &cmd);

  struct netCtx *nc = malloc(sizeof(*nc));
  nc->curIdx = 0;
  nc->current = NULL;
  nc->it = MR_Iterate(cg, netCursorCallback, NULL);
  ResultProcessor *proc = NewResultProcessor(NULL, nc);
  proc->Free = net_Free;
  proc->Next = net_Next;
  return proc;
}

int getAggregateFields(FieldList *l, RedisModuleCtx *ctx, CmdArg *cmd);
ResultProcessor *buildGroupBy(CmdArg *grp, RedisSearchCtx *sctx, ResultProcessor *upstream,
                              char **err);
ResultProcessor *buildSortBY(CmdArg *srt, ResultProcessor *upstream, char **err);
ResultProcessor *buildProjection(CmdArg *arg, ResultProcessor *upstream, RedisSearchCtx *sctx,
                                 char **err);
ResultProcessor *addLimit(CmdArg *arg, ResultProcessor *upstream, char **err);
static ResultProcessor *Aggregate_BuildDistributedChain(QueryPlan *plan, void *ctx, char **err) {

  CmdArg *cmd = ctx;

  MRCommand xcmd = MR_NewCommand(6, "_FT.AGGREGATE", CMDARG_STRPTR(CmdArg_FirstOf(cmd, "idx")),
                                 CMDARG_STRPTR(CmdArg_FirstOf(cmd, "query")), "WITHCURSOR", "COUNT",
                                 RESULTS_PER_ITERATION);
  if (getAggregateFields(&plan->opts.fields, plan->ctx->redisCtx, cmd)) {

    char buf[1024];

    for (int i = 0; i < plan->opts.fields.numFields; i++) {
      snprintf(buf, sizeof(buf), "@%s", plan->opts.fields.fields[i].name);
      MRCommand_AppendArgs(&xcmd, 4, "APPLY", buf, "AS", buf);
    }
  }  // The base processor translates index results into search results
  MRCommand_FPrint(stderr, &xcmd);
  ResultProcessor *next = NewNetworkFetcher(plan->ctx, xcmd, GetSearchCluster());
  ResultProcessor *prev = NULL;
  next->ctx.qxc = &plan->execCtx;
  // Walk the children and evaluate them
  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    prev = next;
    if (!strcasecmp(key, "GROUPBY")) {
      next = buildGroupBy(child, plan->ctx, next, err);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = buildSortBY(child, next, err);
    } else if (!strcasecmp(key, "APPLY")) {
      next = buildProjection(child, next, plan->ctx, err);
    } else if (!strcasecmp(key, "LIMIT")) {
      next = addLimit(child, next, err);
    }
    if (!next) {
      goto fail;
    }
  }

  return next;

fail:
  if (prev) {
    ResultProcessor_Free(prev);
  }

  RedisModule_Log(plan->ctx->redisCtx, "warning", "Could not parse aggregate reuqest: %s", *err);
  return NULL;
}
CmdArg *Aggregate_ParseRequest(RedisModuleString **argv, int argc, char **err);
int AggregateRequest_BuildDistributedPlan(AggregateRequest *req, RedisSearchCtx *sctx,
                                          RedisModuleString **argv, int argc, const char **err) {

  req->args = Aggregate_ParseRequest(argv, argc, (char **)err);
  if (!req->args) {
    *err = strdup("Could not parse aggregate request");
    return REDISMODULE_ERR;
  }

  RSSearchOptions opts = RS_DEFAULT_SEARCHOPTS;
  // mark the query as an aggregation query
  opts.flags |= Search_AggregationQuery;
  opts.concurrentMode = 0;
  req->plan =
      Query_BuildPlan(sctx, NULL, &opts, Aggregate_BuildDistributedChain, req->args, (char **)&err);
  if (!req->plan) {
    *err = strdup(QUERY_ERROR_INTERNAL_STR);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
