
#include <result_processor.h>
#include <dep/rmr/rmr.h>
#include <search_cluster.h>
#include <query_plan.h>
#include <commands.h>
#include <aggregate/aggregate.h>

int netCursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {
  fprintf(stderr, "Got reply for ");
  MRCommand_FPrint(stderr, cmd);
  // MRReply_Print(stderr, rep);
  // putc('\n', stderr);
  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 2) {
    int rc = MRIteratorCallback_Done(ctx, 1);
    fprintf(stderr, "Invalid response %d!\n", rc);

    return REDIS_ERR;
  }

  MRReply *arr = MRReply_ArrayElement(rep, 0);
  if (arr && MRReply_Type(arr) == MR_REPLY_ARRAY) {

    size_t len = MRReply_Length(arr);
    for (size_t i = 0; i < len; i++) {
      MRIteratorCallback_AddReply(ctx, MRReply_StealArrayElement(arr, i));
    }
    fprintf(stderr, "Pushed %d responses to channeln\n", len);
  } else {
    MRIteratorCallback_Done(ctx, 1);
  }
  long long curs = 0;
  if (MRReply_ToInteger(MRReply_ArrayElement(rep, 1), &curs) && curs > 0) {
    if (strcasecmp(cmd->args[0], "_FT.CURREAD")) {

      char buf[128];
      sprintf(buf, "%lld", curs);
      const char *idx = cmd->args[1];
      MRCommand newCmd = MR_NewCommand(4, "_FT.CURREAD", idx, buf, "5000");
      MRCommand_Free(cmd);
      *cmd = newCmd;
    }
    MRCommand_FPrint(stderr, cmd);
    MRIteratorCallback_ResendCommand(ctx, cmd);
  } else {
    MRIteratorCallback_Done(ctx, 0);
  }
  return REDIS_OK;
}

RSValue *MRReply_ToValue(MRReply *r) {
  RSValue *v = RS_NullVal();
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      char *s = MRReply_String(r, &l);
      v = RS_StringValT(s, l, RSString_Const);
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

static int net_Next(ResultProcessorCtx *ctx, SearchResult *r) {

  MRIterator *it = ctx->privdata;
  MRReply *rep;
  do {
    rep = MRIterator_Next(it);
    if (rep == MRITERATOR_DONE) {
      fprintf(stderr, "WE ARE DONE!\n");
      return RS_RESULT_EOF;
    }
  } while (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY);
  fprintf(stderr, "Got response!\n");
  // if (r->fields) {
  //   RSFieldMap_Reset(r->fields);
  // } else {
  r->fields = RS_NewFieldMap(8);
  //
  //}

  for (int i = 0; i < MRReply_Length(rep); i += 2) {
    MRReply_Print(stderr, r);
    RSValue *v = MRReply_ToValue(MRReply_ArrayElement(rep, i + 1));
    const char *c = MRReply_String(MRReply_ArrayElement(rep, i), NULL);
    printf("%s => ", c);
    RSValue_Print(v);
    printf("\n");
    RSFieldMap_Set(&r->fields, c, v);
  }
  return RS_RESULT_OK;
}

void net_Free(ResultProcessor *rp) {
  // MRIterator *it = rp->ctx.privdata;
  // TODO: FREE
  // MRIterator_Free(it);
  free(rp);
}
ResultProcessor *NewNetworkFetcher(RedisSearchCtx *sctx, MRCommand cmd, SearchCluster *sc) {

  MRCommand_FPrint(stderr, &cmd);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(sc, &cmd);

  MRIterator *it = MR_Iterate(cg, netCursorCallback, NULL);
  ResultProcessor *proc = NewResultProcessor(NULL, it);
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

  MRCommand xcmd = MR_NewCommand(5, "_FT.AGGREGATE", CMDARG_STRPTR(CmdArg_FirstOf(cmd, "idx")),
                                 CMDARG_STRPTR(CmdArg_FirstOf(cmd, "query")), "WITHCURSOR", "100");
  if (getAggregateFields(&plan->opts.fields, plan->ctx->redisCtx, cmd)) {
    MRCommand_AppendArgs(&xcmd, 3, "LOAD", "1", "@actor");

    // for (int i = 0; i < plan->opts.fields.numFields; i++) {
    //   MRCommand_AppendArgs(&xcmd, 1, plan->opts.fields.fields->name);
    //}
  }  // The base processor translates index results into search results
  MRCommand_FPrint(stderr, &xcmd);
  ResultProcessor *next = NewNetworkFetcher(plan->ctx, xcmd, GetSearchCluster());
  ResultProcessor *prev = NULL;
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

  req->plan =
      Query_BuildPlan(sctx, NULL, &opts, Aggregate_BuildDistributedChain, req->args, (char **)&err);
  if (!req->plan) {
    *err = strdup(QUERY_ERROR_INTERNAL_STR);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
