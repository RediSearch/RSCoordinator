#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libuv.h"

#include "rmr.h"
#include "redismodule.h"
#include "cluster.h"

/* Copy a redisReply object */
MRReply *mrReply_Duplicate(redisReply *rep);

/* Currently a single cluster is supported */
static MRCluster *__cluster = NULL;

/* MapReduce context for a specific command's execution */
typedef struct MRCtx {
  time_t startTime;
  size_t numReplied;
  MRReply **replies;
  MRReduceFunc reducer;
  void *privdata;
} MRCtx;


/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(void *ctx) {
  MRCtx *ret = malloc(sizeof(MRCtx));
  ret->startTime = time(NULL);
  ret->numReplied = 0;
  ret->replies = calloc(__cluster->numNodes, sizeof(redisReply *));
  ret->reducer = NULL;
  ret->privdata = ctx;
  return ret;
}

void MRCtx_Free(MRCtx *ctx) {
  // clean up the replies
  for (int i = 0; i < ctx->numReplied; i++) {
    if (ctx->replies[i] != NULL) {
      MRReply_Free(ctx->replies[i]);
    }
  }
  free(ctx->replies);

  // free the context
  free(ctx);
}

/* Get the user stored private data from the context */
void *MRCtx_GetPrivdata(struct MRCtx *ctx) { return ctx->privdata; }


/* handler for unblocking redis commands, that calls the actual reducer */
int __mrUnblockHanlder(RedisModuleCtx *ctx, RedisModuleString **argv,
                       int argc) {

  MRCtx *mc = RedisModule_GetBlockedClientPrivateData(ctx);
  mc->privdata = ctx;

  int rc = mc->reducer(mc, mc->numReplied, mc->replies);

  MRCtx_Free(mc);
  return rc;
}

void __mrFreePrivdata(void *privdata) { free(privdata); }


/* The callback called from each fanout request to aggregate their replies */
static void fanoutCallback(redisAsyncContext *c, void *r, void *privdata) {

  MRCtx *ctx = privdata;

  ctx->replies[ctx->numReplied++] = mrReply_Duplicate(r);
  
  // If we've received the last reply - unblock the client
  if (ctx->numReplied == __cluster->numNodes) {
    RedisModuleBlockedClient *bc = ctx->privdata;
    RedisModule_UnblockClient(bc, ctx);
  }
}


/* start the event loop side thread */
void *sideThread(void *arg) {
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);
  return NULL;
}

static pthread_t loop_th;

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRNodeProvider np) {

  __cluster = MR_NewCluster(np);
  MRCluster_ConnectAll(__cluster);

  if (pthread_create(&loop_th, NULL, sideThread, NULL) != 0) {
    perror("thread creat");
    exit(-1);
  }
}

// temporary request context to pass to the event loop
struct __mrRequestCtx {
  MRCtx *ctx;
  MRReduceFunc f;
  int argc;
  const char **argv;
};

/* The map request received in the event loop in a thread safe manner */
void __uvMapRequest(uv_work_t *wr) {

  struct __mrRequestCtx *mc = wr->data;

  mc->ctx->numReplied = 0;
  mc->ctx->reducer = mc->f;

  for (int i = 0; i < __cluster->numNodes; i++) {

    redisAsyncContext *rcx = __cluster->conns[i];
    redisAsyncCommandArgv(rcx, fanoutCallback, mc->ctx, mc->argc, mc->argv,
                          NULL);
    // TODO: handle errors
  }

  free(mc);

  // return REDIS_OK;
}


/* Fanout map - send the same command to all the shards, sending the collective reply to the reducer callback */
int MR_Fanout(struct MRCtx *ctx, MRReduceFunc reducer, int argc, const char **argv) {

  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->argv = argv;
  rc->argc = argc;

  RedisModuleCtx *rx = ctx->privdata;
  ctx->privdata =
      RedisModule_BlockClient(rx, __mrUnblockHanlder, NULL, NULL, 0);

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvMapRequest, NULL);
  return 0;
}
