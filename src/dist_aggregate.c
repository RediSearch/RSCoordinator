
#include <result_processor.h>
#include <dep/rmr/rmr.h>
#include <search_cluster.h>

int cursorCallback(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd) {

  if (!rep || MRReply_Type(rep) != MR_REPLY_ARRAY || MRReply_Length(rep) != 2) {
    MRIteratorCallback_Done(ctx, 1);
    return REDIS_ERR;
  }

  MRReply *arr = MRReply_ArrayElement(rep, 0);
  if (arr && MRReply_Type(arr) == MR_REPLY_ARRAY) {

    size_t len = MRReply_Length(arr);
    for (size_t i = 0; i < len; i++) {
      MRIteratorCallback_AddReply(ctx, MRReply_StealArrayElement)
    }
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
    // MRCommand_FPrint(stderr, cmd);
    MRIteratorCallback_ResendCommand(ctx, cmd);
  } else {
    MRIteratorCallback_Done(ctx, 0);
  }
  return REDIS_OK;
}

RSValue *MRReply_ToValue(MRReply *r) {
  RSValue *v = RS_NullVal();
  switch (MRReply_Type(r)) {
    case MR_REPLY_STRING:
    case MR_REPLY_STATUS: {
      size_t l;
      char *s = MRReply_String(r, &l);
      v = RS_StringVal(s, l);
      break;
    };
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
      break;
  }
  return v;
}

static int net_Next(ResultProcessorCtx *ctx, SearchResult *r) {

  MRIterator *it = ctx->privdata;
  MRReply *rep;
  do {
    MRReply *rep = MRIterator_Next(it);
    if (rep == MRITERATOR_DONE) {
      return RS_RESULT_EOF;
    }
    if (MRReply_Type(rep) != MR_REPLY_ARRAY) continue;
  } while (1);

  if (r->fields) {
    RSFieldMap_Reset(r->fields);
  } else {
    r->fields = RS_NewFieldMap(8);
  }

  for (int i = 0; i < MRReply_Length(rep); i += 2) {
    RSValue *v = MRReply_ToValue(MRReply_ArrayElement(rep, i + 1));
    RSFieldMap_Add(&r->fields, MRReply_String(MRReply_ArrayElement(rep, i), NULL), v);
  }
  return RS_RESULT_OK;
}

void net_Free(ResultProcessor *rp) {
  MRIterator *it = rp->ctx.privdata;
  // TODO: FREE
  // MRIterator_Free(it);
  free(rp);
}
ResultProcessor *NewNetworkFetcher(RedisSearchCtx *sctx, MRCommand cmd, SearchCluster *sc) {

  MRCommand_FPrint(stderr, &cmd);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(sc, &cmd);

  MRIterator *it = MR_Iterate(cg, cursorCallback, NULL);
  ResultProcessor *proc = NewResultProcessor(NULL, it);
  proc->Free = net_Free;
  proc->Next = net_Next;
}