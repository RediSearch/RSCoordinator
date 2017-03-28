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
} MRCtx;

int MR_UpdateTopology(void *ctx) {
  printf("Updating topo!\n");
  if (!__cluster->topo) {
    MRCLuster_UpdateTopology(__cluster, ctx);
  }

  printf("Not really updating topo...\n");
  return REDIS_OK;
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

/* start the event loop side thread */
void *sideThread(void *arg) {
  // uv_loop_configure(uv_default_loop(), UV_LOOP_BLOCK_SIGNAL)
  while (1) {
    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT)) break;
    usleep(1000);
  }
  printf("Uv loop exited!\n");
  return NULL;
}

static pthread_t loop_th;

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl) {

  __cluster = cl;
  // MRCluster_ConnectAll(__cluster);
  printf("Creating thread...\n");
  if (pthread_create(&loop_th, NULL, sideThread, NULL) != 0) {
    perror("thread create");
    exit(-1);
  }
  printf("Thread created\n");
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
  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  if (__cluster->topo) {
    for (int i = 0; i < __cluster->topo->numShards; i++) {
      printf("Sending %d/%zd\n", i, __cluster->topo->numShards);
      MRCommand *cmd = &mc->cmds[0];
      if (MRCluster_SendCommand(__cluster, cmd, fanoutCallback, mrctx) == REDIS_OK) {
        mrctx->numExpected++;
      }
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

void __uvMapRequest(uv_work_t *wr) {

  struct __mrRequestCtx *mc = wr->data;

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;
  for (int i = 0; i < mc->numCmds; i++) {

    if (MRCluster_SendCommand(__cluster, &mc->cmds[i], fanoutCallback, mrctx) == REDIS_OK) {
      mc->ctx->numExpected++;
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

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvFanoutRequest, NULL);
  return 0;
}

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommandGenerator cmds) {
  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(cmds.Len(cmds.ctx), sizeof(MRCommand));
  rc->numCmds = cmds.Len(cmds.ctx);
  for (int i = 0; i < rc->numCmds; i++) {
    if (!cmds.Next(cmds.ctx, &rc->cmds[i])) {
      rc->numCmds = i;
      break;
    }
  }

  ctx->redisCtx = RedisModule_BlockClient(ctx->redisCtx, __mrUnblockHanlder, NULL, NULL, 0);

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvMapRequest, NULL);
  return 0;
}

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {
  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = MRCommand_Copy(&cmd);

  ctx->redisCtx = RedisModule_BlockClient(ctx->redisCtx, __mrUnblockHanlder, NULL, NULL, 0);

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvMapRequest, NULL);
  return 0;
}