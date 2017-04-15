#include "periodic.h"

void _periodicTimerCB( uv_timer_t *tm, int status);
void _periodicCmdCallbabk(redisAsyncContext *c, void *r, void *privdata);

/* Timer loop for retrying disconnected connections */
void _periodicTimerCB( uv_timer_t *tm, int status) {
  RSPeriodicCommand *pc = tm->data;
  if (MRConn_SendCommand(pc->conn, &pc->cmd, _periodicCmdCallbabk, pc) == REDIS_ERR) {
    uv_timer_start(&pc->timer, _periodicTimerCB, pc->interval, 0);
  }
  // if there was no error, the periodic command response will retrigger the timer
}

void _periodicCmdCallbabk(redisAsyncContext *c, void *r, void *privdata) {
  RSPeriodicCommand *pc = privdata;

  if (r) {
    int rc = pc->commandHandler(pc->handlerCtx, r, &pc->cmd);
    if (rc == 0) {
      printf("Stopping periodic loop");
      return;
    }
  }

  // retrigger the timer
  uv_timer_start(&pc->timer, _periodicTimerCB, pc->interval, 0);
}

RSPeriodicCommand *NewPeriodicCommandRunner(MRCommand *cmd, MREndpoint *ep, uint64_t interval,
                                            RSPeriodicCommandHandler handler, void *handlerCtx) {

  RSPeriodicCommand *ret = malloc(sizeof(*ret));
  ret->cmd = MRCommand_Copy(cmd);
  MREndpoint_Copy(&ret->endpoint, ep);
  ret->conn = MR_NewConn(&ret->endpoint);
  ret->commandHandler = handler;
  ret->handlerCtx = handlerCtx;
  ret->interval = interval;
  uv_timer_init(uv_default_loop(), &ret->timer);
  ret->timer.data = ret;
  MRConn_Connect(ret->conn);

  uv_timer_start(&ret->timer, _periodicTimerCB, ret->interval, 0);
  return ret;
}

int periodicGCHandler(void *ctx, redisReply *rep, MRCommand *cmd) {

  //char *indexName = ctx;

  if (rep->type == REDIS_REPLY_ERROR) {
    //printf("Got error: %s\n", rep->str);
  } else if (rep->type == REDIS_REPLY_ARRAY && rep->elements == 3) {
    char *indexName = rep->element[0]->str;
    char *term = rep->element[1]->str;
    long long offset = rep->element[2]->integer;
    //printf("Got response: %s %s %lld\n", indexName, term, offset);

    if (offset != 0) {
      char buf[24];
      sprintf(buf, "%lld", offset);
      MRCommand_Free(cmd);
      *cmd = MR_NewCommand(4, "FT.REPAIR", indexName, term, buf);
      MRCommand_Print(cmd);
    } else {
      if (cmd->num != 1) {
        MRCommand_Free(cmd);
        *cmd = MR_NewCommand(1, "FT.REPAIR");
      }
    }
  }
  return 1;
}
