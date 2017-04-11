#ifndef __RS_PERIODIC_H__
#define __RS_PERIODIC_H__

#include <uv.h>
#include <redismodule.h>
#include <dep/rmr/conn.h>

typedef int (*RSPeriodicCommandHandler)(void *ctx, redisReply *rep, MRCommand *cmd);
typedef struct {
  MREndpoint endpoint;
  MRConn *conn;
  MRCommand cmd;
  uint64_t interval;
  uv_timer_t timer;
  /* Callback to process the command response, may alter the command for the next send. If it
   * returns 0 we stop the loop */
  RSPeriodicCommandHandler commandHandler;
  void *handlerCtx;
} RSPeriodicCommand;

RSPeriodicCommand *NewPeriodicCommandRunner(MRCommand *cmd, MREndpoint *ep, uint64_t interval,
                                            RSPeriodicCommandHandler handler, void *handlerCtx);

int periodicGCHandler(void *ctx, redisReply *rep, MRCommand *cmd);

#endif