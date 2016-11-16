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
  int numReplied;
  int numExpected;
  MRReply **replies;
  MRReduceFunc reducer;
  void *privdata;
} MRCtx;

/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(void *ctx) {
  MRCtx *ret = malloc(sizeof(MRCtx));
  ret->startTime = time(NULL);
  ret->numReplied = 0;
  ret->numExpected = 0;
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
  if (ctx->numReplied == ctx->numReplied) {
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
void MR_Init(MRNodeProvider np, ShardFunc sf) {

  __cluster = MR_NewCluster(np, sf);
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
  MRCommand *cmds;
  int numCmds;
};

/* The fanout request received in the event loop in a thread safe manner */
void __uvFanoutRequest(uv_work_t *wr) {

  struct __mrRequestCtx *mc = wr->data;

  mc->ctx->numReplied = 0;
  mc->ctx->numReplied = __cluster->numNodes;
  mc->ctx->reducer = mc->f;

  for (int i = 0; i < __cluster->numNodes; i++) {

    redisAsyncContext *rcx = __cluster->conns[i];
    redisAsyncCommandArgv(rcx, fanoutCallback, mc->ctx, mc->cmds[0].num,
                          (const char **)mc->cmds[0].args, NULL);
    // TODO: handle errors
  }

  for (int i = 0; i < mc->numCmds; i++) {
    MRCommand_Free(&mc->cmds[i]);
  }
  free(mc->cmds);
  free(mc);

  // return REDIS_OK;
}

void __uvMapRequest(uv_work_t *wr) {

  struct __mrRequestCtx *mc = wr->data;

  mc->ctx->numReplied = 0;
  mc->ctx->reducer = mc->f;
  mc->ctx->numExpected = 0;
  for (int i = 0; i < mc->numCmds; i++) {

    int shard =
        __cluster->sharder(&mc->cmds[i], __cluster->nodes, __cluster->numNodes);
    printf("Shard for %s %s: %d\n", mc->cmds[i].args[0], mc->cmds[i].args[1],
           shard);
    if (shard >= 0) {

      redisAsyncContext *rcx = __cluster->conns[shard];
      redisAsyncCommandArgv(rcx, fanoutCallback, mc->ctx, mc->cmds[i].num,
                            (const char **)mc->cmds[i].args, NULL);
      mc->ctx->numExpected++;
      // TODO: handle errors
    }
  }

  for (int i = 0; i < mc->numCmds; i++) {
    MRCommand_Free(&mc->cmds[i]);
  }
  free(mc->cmds);
  free(mc);

  // return REDIS_OK;
}

/* Fanout map - send the same command to all the shards, sending the collective
 * reply to the reducer callback */
int MR_Fanout(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {

  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;

  RedisModuleCtx *rx = ctx->privdata;
  ctx->privdata =
      RedisModule_BlockClient(rx, __mrUnblockHanlder, NULL, NULL, 0);

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvFanoutRequest, NULL);
  return 0;
}

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand *cmds, int num) {
  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(num, sizeof(MRCommand));
  rc->numCmds = num;
  for (int i = 0; i < num; i++) {
    rc->cmds[i] = cmds[i];
  }

  RedisModuleCtx *rx = ctx->privdata;
  ctx->privdata =
      RedisModule_BlockClient(rx, __mrUnblockHanlder, NULL, NULL, 0);

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvMapRequest, NULL);
  return 0;
}
