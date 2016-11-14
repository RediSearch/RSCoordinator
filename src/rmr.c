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

typedef struct MREndpoint {
  char *host;
  int port;
  redisAsyncContext *c;
} MREndpoint;

typedef struct MRCluster {
  int numNodes;
  MREndpoint *nodes;

} MRCluster;

// a single request
typedef struct MRCtx {
  time_t startTime;
  size_t numReplied;
  redisReply **replies;
  MRReduceFunc reducer;
  MRCluster *cluster;

  void *privdata;
} MRCtx;

MRCluster *MR_NewCluster(int numNodes, MREndpoint *nodes) {
  MRCluster *cl = malloc(sizeof(MRCluster));
  cl->numNodes = numNodes;
  cl->nodes = nodes;
  return cl;
}

void connectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    return;
  }
  printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    return;
  }
  printf("Disconnected...\n");
}

void MRCluster_ConnectAll(MRCluster *cl) {

  for (int i = 0; i < cl->numNodes; i++) {
    redisAsyncContext *c =
        redisAsyncConnect(cl->nodes[i].host, cl->nodes[i].port);
        
    if (c->err) {
      return;
    }
    c->data = cl;

    redisLibuvAttach(c, uv_default_loop());
    redisAsyncSetConnectCallback(c, connectCallback);
    redisAsyncSetDisconnectCallback(c, disconnectCallback);
    cl->nodes[i].c = c;
  }
}

MRCtx *MR_CreateCtx(MRCluster *cl, void *ctx) {
  MRCtx *ret = malloc(sizeof(MRCtx));
  ret->startTime = time(NULL);
  ret->numReplied = 0;
  ret->replies = calloc(cl->numNodes, sizeof(redisReply *));
  ret->cluster = cl;
  ret->reducer = NULL;
  ret->privdata = ctx;
  return ret;
}

void *MRCtx_GetPrivdata(struct MRCtx *ctx) {
    return ctx->privdata;
}

int __mrUnblockHanlder(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  MRCtx *mc = RedisModule_GetBlockedClientPrivateData(ctx);
  mc->privdata = ctx;

  return mc->reducer(mc, mc->numReplied, mc->replies);
  
}

void __mrFreePrivdata(void *privdata) {
    free(privdata);
}


void fanoutCallback(redisAsyncContext *c, void *r, void *privdata) {

  MRCtx *ctx = privdata;
  ctx->replies[ctx->numReplied++] = r;
  printf("Received %d/%d replies\n", (int)ctx->numReplied, (int)ctx->cluster->numNodes);

  if (ctx->numReplied == ctx->cluster->numNodes) {
    RedisModuleBlockedClient *bc = ctx->privdata;
    RedisModule_UnblockClient(bc, ctx);
  }
}

// fire a new request cycle
int fanoutRequest(MRCtx *ctx, MRReduceFunc reducer, int argc,
                  const char **argv) {

  // int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void
  // *privdata, int argc, const char **argv, const size_t *argvlen);

  ctx->numReplied = 0;
  ctx->reducer = reducer;

  for (int i = 0; i < ctx->cluster->numNodes; i++) {

    redisAsyncContext *rcx = ctx->cluster->nodes[i].c;

    int rc = redisAsyncCommandArgv(rcx, fanoutCallback, ctx, argc, argv, NULL);
    if (rc != REDIS_OK) {
      return rc;
    }
  }

  return REDIS_OK;
}


void tcb(uv_timer_t *t) {
    printf("TBC!\n");
}
void *sideThread(void *arg) {
  // ev_run(EV_DEFAULT_ EVBACKEND_KQUEUE);
   uv_timer_t timer_req;

    uv_timer_init(uv_default_loop(), &timer_req);
    uv_timer_start(&timer_req, tcb, 5000, 2000);
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);
  printf("finished running uv loop %d\n", uv_loop_alive(uv_default_loop()));
  
  return NULL;
}

 pthread_t th;

void MR_Init(MRCluster *cl) {

  signal(SIGPIPE, SIG_IGN);

 MRCluster_ConnectAll(cl);

 //MRCluster_ConnectAll(cl);
  if (pthread_create(&th, NULL, sideThread, cl) != 0) {
    perror("thread creat");
    exit(-1);
  }

  //sleep(1);

  // ev_run(EV_DEFAULT_ EVBACKEND_KQUEUE);
  // uv_run(uv_default_loop(), UV_RUN_DEFAULT);
}

struct __mrRequestCtx {
  MRCtx *ctx;
  MRReduceFunc f;
  int argc;
  const char **argv;
};

void __uvMapRequest(uv_work_t *wr) {

  struct __mrRequestCtx *mc = wr->data;

  mc->ctx->numReplied = 0;
  mc->ctx->reducer = mc->f;

  for (int i = 0; i < mc->ctx->cluster->numNodes; i++) {

    redisAsyncContext *rcx = mc->ctx->cluster->nodes[i].c;

    int rc = redisAsyncCommandArgv(rcx, fanoutCallback, mc->ctx, mc->argc,
                                   mc->argv, NULL);
    // TODO: handle errors
    // if (rc != REDIS_OK) {
    //     return rc;
    // }
  }

  // return REDIS_OK;
}
int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, int argc,
           const char **argv) {

  struct __mrRequestCtx *rc = malloc(sizeof(struct __mrRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->argv = argv;
  rc->argc = argc;
  
  RedisModuleCtx *rx = ctx->privdata;
  ctx->privdata = RedisModule_BlockClient(rx, __mrUnblockHanlder, NULL, NULL, 0);

  uv_work_t *wr = malloc(sizeof(uv_work_t));
  wr->data = rc;
  uv_queue_work(uv_default_loop(), wr, __uvMapRequest, NULL);
  return 0;
}

