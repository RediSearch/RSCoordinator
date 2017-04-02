#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/param.h>

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
  int numErrored;
  MRReply **replies;
  int repliesCap;
  MRReduceFunc reducer;
  void *privdata;
  void *redisCtx;
  MRCoordinationStrategy strategy;
} MRCtx;

void MR_SetCoordinationStrategy(MRCtx *ctx, MRCoordinationStrategy strategy) {
  ctx->strategy = strategy;
}
int MR_UpdateTopology(void *ctx) {
  // printf("Updating topo!\n");
  return MRCLuster_UpdateTopology(__cluster, ctx);
}

/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(RedisModuleCtx *ctx, void *privdata) {
  MRCtx *ret = malloc(sizeof(MRCtx));
  ret->startTime = time(NULL);
  ret->numReplied = 0;
  ret->numErrored = 0;
  ret->numExpected = 0;
  ret->repliesCap = MAX(1, MRCluster_NumShards(__cluster));
  ret->replies = calloc(ret->repliesCap, sizeof(redisReply *));
  ret->reducer = NULL;
  ret->privdata = privdata;
  ret->strategy = MRCluster_FlatCoordination;
  ret->redisCtx = ctx;
  return ret;
}

void MRCtx_Free(MRCtx *ctx) {
  // clean up the replies
  for (int i = 0; i < ctx->numReplied; i++) {
    if (ctx->replies[i] != NULL) {
      MRReply_Free(ctx->replies[i]);
      ctx->replies[i] = NULL;
    }
  }
  free(ctx->replies);

  // free the context
  free(ctx);
}

/* Get the user stored private data from the context */
void *MRCtx_GetPrivdata(struct MRCtx *ctx) {
  return ctx->privdata;
}

RedisModuleCtx *MRCtx_GetRedisCtx(struct MRCtx *ctx) {
  return ctx->redisCtx;
}

/* handler for unblocking redis commands, that calls the actual reducer */
int __mrUnblockHanlder(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  MRCtx *mc = RedisModule_GetBlockedClientPrivateData(ctx);
  mc->redisCtx = ctx;

  int rc = mc->reducer(mc, mc->numReplied, mc->replies);

  MRCtx_Free(mc);
  return rc;
}

// TODO: make this user configrable
void __mrFreePrivdata(void *privdata) {
  free(privdata);
}

/* The callback called from each fanout request to aggregate their replies */
static void fanoutCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRCtx *ctx = privdata;
  if (!r) {
    ctx->numErrored++;

  } else {
    redisReply *rp = (redisReply *)mrReply_Duplicate(r);

    /* If needed - double the capacity for replies */
    if (ctx->numReplied == ctx->repliesCap) {
      ctx->repliesCap *= 2;
      ctx->replies = realloc(ctx->replies, ctx->repliesCap * sizeof(redisReply *));
    }
    ctx->replies[ctx->numReplied++] = rp;
  }

  // If we've received the last reply - unblock the client
  if (ctx->numReplied + ctx->numErrored == ctx->numExpected) {
    RedisModuleBlockedClient *bc = ctx->redisCtx;
    RedisModule_UnblockClient(bc, ctx);
  }
}

// temporary request context to pass to the event loop
struct __mrRequestCtx {
  MRCtx *ctx;
  MRReduceFunc f;
  MRCommand *cmds;
  int numCmds;
  void (*cb)(struct __mrRequestCtx *);
};

typedef struct {
  struct __mrRequestCtx **queue;
  size_t sz;
  size_t cap;
  uv_mutex_t *lock;
  uv_async_t async;
} __mrRequestQueue;

void __rq_push(__mrRequestQueue *q, struct __mrRequestCtx *req) {
  uv_mutex_lock(q->lock);
  if (q->sz == q->cap) {
    q->cap += q->cap ? q->cap : 1;
    q->queue = realloc(q->queue, q->cap * sizeof(struct __mrRequestCtx *));
  }
  q->queue[q->sz++] = req;

  uv_mutex_unlock(q->lock);
  uv_async_send(&q->async);
}

struct __mrRequestCtx *__rq_pop(__mrRequestQueue *q) {
  uv_mutex_lock(q->lock);
  if (q->sz == 0) {
    uv_mutex_unlock(q->lock);
    return NULL;
  }
  struct __mrRequestCtx *r = q->queue[--q->sz];
  uv_mutex_unlock(q->lock);
  return r;
}

void __rq_async_cb(uv_async_t *async) {
  __mrRequestQueue *q = async->data;
  struct __mrRequestCtx *req;
  while (NULL != (req = __rq_pop(q))) {
    req->cb(req);
  }
}

void __rq_init(__mrRequestQueue *q, size_t cap) {
  q->sz = 0;
  q->cap = cap;
  q->lock = malloc(sizeof(*q->lock));
  uv_mutex_init(q->lock);
  q->queue = calloc(q->cap, sizeof(struct __mrRequestCtx *));
  // TODO: Add close cb
  uv_async_init(uv_default_loop(), &q->async, __rq_async_cb);
  q->async.data = q;
}

__mrRequestQueue __rq;

/* start the event loop side thread */
void sideThread(void *arg) {
  __rq_init(&__rq, 8);

  // uv_loop_configure(uv_default_loop(), UV_LOOP_BLOCK_SIGNAL)
  while (1) {
    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT)) break;
    usleep(1000);
  }
  printf("Uv loop exited!\n");
}

uv_thread_t loop_th;

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl) {

  __cluster = cl;

  // MRCluster_ConnectAll(__cluster);
  printf("Creating thread...\n");

  if (uv_thread_create(&loop_th, sideThread, NULL) != 0) {
    perror("thread create");
    exit(-1);
  }
  printf("Thread created\n");
}

MRClusterTopology *MR_GetCurrentTopology() {
  return __cluster->topo;
}
/* The fanout request received in the event loop in a thread safe manner */
void __uvFanoutRequest(struct __mrRequestCtx *mc) {

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  if (__cluster->topo) {
    for (int i = 0; i < __cluster->topo->numShards; i++) {
      MRCommand *cmd = &mc->cmds[0];
      mrctx->numExpected =
          MRCluster_FanoutCommand(__cluster, mrctx->strategy, cmd, fanoutCallback, mrctx);
    }
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->redisCtx;
    RedisModule_UnblockClient(bc, mrctx);
    // printf("could not send single command. hande fail please\n");
  }

  for (int i = 0; i < mc->numCmds; i++) {
    MRCommand_Free(&mc->cmds[i]);
  }
  free(mc->cmds);
  free(mc);

  // return REDIS_OK;
}

void __uvMapRequest(struct __mrRequestCtx *mc) {

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;
  for (int i = 0; i < mc->numCmds; i++) {

    if (MRCluster_SendCommand(__cluster, mrctx->strategy, &mc->cmds[i], fanoutCallback, mrctx) ==
        REDIS_OK) {
      mrctx->numExpected++;
    }
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->redisCtx;
    RedisModule_UnblockClient(bc, mrctx);
    // printf("could not send single command. hande fail please\n");
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
  rc->ctx->redisCtx = RedisModule_BlockClient(ctx->redisCtx, __mrUnblockHanlder, NULL, NULL, 0);

  rc->cb = __uvFanoutRequest;
  __rq_push(&__rq, rc);
  return REDIS_OK;

  // uv_work_t *wr = malloc(sizeof(uv_work_t));
  // wr->data = rc;
  // uv_queue_work(uv_default_loop(), wr, __uvFanoutRequest, NULL);
  // return 0;
}

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommandGenerator cmds) {
  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(cmds.Len(cmds.ctx), sizeof(MRCommand));
  rc->numCmds = cmds.Len(cmds.ctx);
  // copy the commands from the iterator to the conext's array
  for (int i = 0; i < rc->numCmds; i++) {
    if (!cmds.Next(cmds.ctx, &rc->cmds[i])) {
      rc->numCmds = i;
      break;
    }
  }

  ctx->redisCtx = RedisModule_BlockClient(ctx->redisCtx, __mrUnblockHanlder, NULL, NULL, 0);

  rc->cb = __uvMapRequest;
  __rq_push(&__rq, rc);
  return REDIS_OK;
}

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {
  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = MRCommand_Copy(&cmd);

  ctx->redisCtx = RedisModule_BlockClient(ctx->redisCtx, __mrUnblockHanlder, NULL, NULL, 0);

  rc->cb = __uvMapRequest;
  __rq_push(&__rq, rc);
  return REDIS_OK;
}

/* Return the active cluster's host count */
size_t MR_NumHosts() {
  return __cluster ? MRCluster_NumHosts(__cluster) : 0;
}