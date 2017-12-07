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

/* Copy a redisReply object. Defined in reply.c */
MRReply *MRReply_Duplicate(redisReply *rep);

/* Currently a single cluster is supported */
static MRCluster *cluster_g = NULL;

static int concurrentRequests_g = 0;

/* MapReduce context for a specific command's execution */
typedef struct MRCtx {
  struct timespec startTime;
  struct timespec firstRespTime;
  struct timespec endTime;
  int timeHistogram[1000];
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

/* The request duration in microsecnds, relevant only on the reducer */
int64_t MR_RequestDuration(MRCtx *ctx) {
  return ((int64_t)1000000 * ctx->endTime.tv_sec + ctx->endTime.tv_nsec / 1000) -
         ((int64_t)1000000 * ctx->startTime.tv_sec + ctx->startTime.tv_nsec / 1000);
}

void MR_SetCoordinationStrategy(MRCtx *ctx, MRCoordinationStrategy strategy) {
  ctx->strategy = strategy;
}

static int totalAllocd = 0;
/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(RedisModuleCtx *ctx, void *privdata) {
  MRCtx *ret = malloc(sizeof(MRCtx));
  clock_gettime(CLOCK_REALTIME, &ret->startTime);
  ret->endTime = ret->startTime;
  ret->firstRespTime = ret->startTime;
  memset(ret->timeHistogram, 0, sizeof(ret->timeHistogram));
  ret->numReplied = 0;
  ret->numErrored = 0;
  ret->numExpected = 0;
  ret->repliesCap = MAX(1, MRCluster_NumShards(cluster_g));
  ret->replies = calloc(ret->repliesCap, sizeof(redisReply *));
  ret->reducer = NULL;
  ret->privdata = privdata;
  ret->strategy = MRCluster_FlatCoordination;
  ret->redisCtx = ctx;
  totalAllocd++;

  return ret;
}

void MRCtx_Free(MRCtx *ctx) {

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

static void freePrivDataCB(void *p) {
  // printf("FreePrivData called!\n");
  if (p) {
    MRCtx *mc = p;
    concurrentRequests_g--;
    MRCtx_Free(mc);
  }
}

static int timeoutHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // concurrentRequests_g--;
  fprintf(stderr, "Timeout!\n");
  return RedisModule_ReplyWithError(ctx, "Timeout calling command");
}

/* handler for unblocking redis commands, that calls the actual reducer */
static int unblockHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  MRCtx *mc = RedisModule_GetBlockedClientPrivateData(ctx);
  clock_gettime(CLOCK_REALTIME, &mc->endTime);

  // fprintf(stderr, "Queue size now %d\n", concurrentRequests_g);
  // fprintf(stderr, "Response histogram: ");
  // for (int i = 0; i < 20; i++) {
  //   fprintf(stderr, "%d ", mc->timeHistogram[i]);
  // }
  // fputc('\n', stderr);

  mc->redisCtx = ctx;

  return mc->reducer(mc, mc->numReplied, mc->replies);
}

/* The callback called from each fanout request to aggregate their replies */
static void fanoutCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRCtx *ctx = privdata;

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  long long ms = ((int64_t)1000 * now.tv_sec + now.tv_nsec / 1000000) -
                 ((int64_t)1000 * ctx->startTime.tv_sec + ctx->startTime.tv_nsec / 1000000);
  if (ms < 1000) {
    ctx->timeHistogram[ms]++;
  }

  if (ctx->numReplied == 0 && ctx->numErrored == 0) {
    clock_gettime(CLOCK_REALTIME, &ctx->firstRespTime);
  }
  if (!r) {
    ctx->numErrored++;

  } else {
    redisReply *rp = (redisReply *)MRReply_Duplicate(r);

    /* If needed - double the capacity for replies */
    if (ctx->numReplied == ctx->repliesCap) {
      ctx->repliesCap *= 2;
      ctx->replies = realloc(ctx->replies, ctx->repliesCap * sizeof(MRReply *));
    }
    ctx->replies[ctx->numReplied++] = (MRReply *)rp;
  }

  // printf("Unblocking, replied %d, errored %d out of %d\n", ctx->numReplied, ctx->numErrored,
  //        ctx->numExpected);

  // If we've received the last reply - unblock the client
  if (ctx->numReplied + ctx->numErrored == ctx->numExpected) {
    RedisModuleBlockedClient *bc = ctx->redisCtx;
    RedisModule_UnblockClient(bc, ctx);
  }
}

// temporary request context to pass to the event loop
struct MRRequestCtx {
  void *ctx;
  MRReduceFunc f;
  MRCommand *cmds;
  int numCmds;
  void (*cb)(struct MRRequestCtx *);
};

typedef struct {
  struct MRRequestCtx **queue;
  size_t sz;
  size_t cap;
  uv_mutex_t *lock;
  uv_async_t async;
} MRRequestQueue;

static void RQ_Push(MRRequestQueue *q, struct MRRequestCtx *req) {
  uv_mutex_lock(q->lock);
  if (q->sz == q->cap) {
    q->cap += q->cap ? q->cap : 1;
    q->queue = realloc(q->queue, q->cap * sizeof(struct MRRequestCtx *));
  }
  q->queue[q->sz++] = req;

  uv_mutex_unlock(q->lock);
  uv_async_send(&q->async);
}

static struct MRRequestCtx *RQ_Pop(MRRequestQueue *q) {
  uv_mutex_lock(q->lock);
  if (q->sz == 0) {
    uv_mutex_unlock(q->lock);
    return NULL;
  }
  if (concurrentRequests_g > MR_CONN_POOL_SIZE * 2) {
    // fprintf(stderr, "Cannot proceed - queue size %d\n", concurrentRequests_g);
    uv_mutex_unlock(q->lock);
    return NULL;
  }
  // else {
  //   fprintf(stderr, "queue size %d\n", concurrentRequests_g);
  // }
  struct MRRequestCtx *r = q->queue[--q->sz];

  uv_mutex_unlock(q->lock);
  return r;
}

static void rqAsyncCb(uv_async_t *async) {
  MRRequestQueue *q = async->data;
  struct MRRequestCtx *req;
  while (NULL != (req = RQ_Pop(q))) {
    concurrentRequests_g++;
    req->cb(req);
  }
}

static void RQ_Init(MRRequestQueue *q, size_t cap) {
  q->sz = 0;
  q->cap = cap;
  q->lock = malloc(sizeof(*q->lock));
  uv_mutex_init(q->lock);
  q->queue = calloc(q->cap, sizeof(struct MRRequestCtx *));
  // TODO: Add close cb
  uv_async_init(uv_default_loop(), &q->async, rqAsyncCb);
  q->async.data = q;
}

MRRequestQueue rq_g;

/* start the event loop side thread */
static void sideThread(void *arg) {
  RQ_Init(&rq_g, 8);

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

  cluster_g = cl;

  // MRCluster_ConnectAll(cluster_g);
  printf("Creating thread...\n");

  if (uv_thread_create(&loop_th, sideThread, NULL) != 0) {
    perror("thread create");
    exit(-1);
  }
  printf("Thread created\n");
}

MRClusterTopology *MR_GetCurrentTopology() {
  return cluster_g ? cluster_g->topo : NULL;
}

MRClusterNode *MR_GetMyNode() {
  return cluster_g ? cluster_g->myNode : NULL;
}

/* The fanout request received in the event loop in a thread safe manner */
static void uvFanoutRequest(struct MRRequestCtx *mc) {

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  if (cluster_g->topo) {
    MRCommand *cmd = &mc->cmds[0];
    mrctx->numExpected =
        MRCluster_FanoutCommand(cluster_g, mrctx->strategy, cmd, fanoutCallback, mrctx);
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
}

static void uvMapRequest(struct MRRequestCtx *mc) {

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;
  for (int i = 0; i < mc->numCmds; i++) {

    if (MRCluster_SendCommand(cluster_g, mrctx->strategy, &mc->cmds[i], fanoutCallback, mrctx) ==
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

  struct MRRequestCtx *rc = malloc(sizeof(struct MRRequestCtx));
  ctx->redisCtx =
      RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 500);
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;

  rc->cb = uvFanoutRequest;
  RQ_Push(&rq_g, rc);
  return REDIS_OK;

  // uv_work_t *wr = malloc(sizeof(uv_work_t));
  // wr->data = rc;
  // uv_queue_work(uv_default_loop(), wr, uvFanoutRequest, NULL);
  // return 0;
}

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommandGenerator cmds) {
  struct MRRequestCtx *rc = malloc(sizeof(struct MRRequestCtx));
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

  ctx->redisCtx =
      RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 500);

  rc->cb = uvMapRequest;
  RQ_Push(&rq_g, rc);
  return REDIS_OK;
}

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {
  struct MRRequestCtx *rc = malloc(sizeof(struct MRRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;

  ctx->redisCtx =
      RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 500);

  rc->cb = uvMapRequest;
  RQ_Push(&rq_g, rc);
  return REDIS_OK;
}

/* Return the active cluster's host count */
size_t MR_NumHosts() {
  return cluster_g ? MRCluster_NumHosts(cluster_g) : 0;
}

/* on-loop update topology request. This can't be done from the main thread */
static void uvUpdateTopologyRequest(struct MRRequestCtx *mc) {
  MRCLuster_UpdateTopology(cluster_g, (MRClusterTopology *)mc->ctx);
  concurrentRequests_g--;
  free(mc);
}

/* Set a new topology for the cluster */
int MR_UpdateTopology(MRClusterTopology *newTopo) {
  if (cluster_g == NULL) {
    return REDIS_ERR;
  }
  // enqueue a request on the io thread, this can't be done from the main thread
  struct MRRequestCtx *rc = calloc(1, sizeof(*rc));
  rc->ctx = newTopo;
  rc->cb = uvUpdateTopologyRequest;
  RQ_Push(&rq_g, rc);
  return REDIS_OK;
}
