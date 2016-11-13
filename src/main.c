#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libev.h"
#include "redismodule.h"


typedef struct {
    char *host;
    int port;
    redisAsyncContext *c;
} Endpoint;

typedef struct {
    int numNodes;
    Endpoint **nodes;
} Cluster;

// a single request
typedef struct {
    time_t startTime;
    size_t numReplied;
    redisReply **replies;
    Cluster *cluster;
} MRCtx;

MRCtx *NewMRCtx(Cluster *cl) {
    MRCtx *ret = malloc(sizeof(MRCtx));
    ret->startTime = time(NULL);
    ret->numReplied = 0;
    ret->replies = calloc(cl->numNodes, sizeof(redisReply*));
    ret->cluster = cl;
    
    return ret;
}


void getCallback(redisAsyncContext *c, void *r, void *privdata);

typedef int (*MRReduceFunc)(MRCtx *ctx, int count, redisReply **replies);

typedef struct {
    MRCtx *ctx;
    MRReduceFunc reducer;
}__mrRequestCtx;

// fire a new request cycle
int fanoutRequest(MRCtx *ctx, int argc, char **argv, MRReduceFunc reducer) {

    
    //int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, int argc, const char **argv, const size_t *argvlen);

    ctx->numReplied = 0;
    for (int i = 0; i < ctx->cluster->numNodes; i++) {
        
        redisAsyncContext *rcx = ctx->cluster->nodes[i]->c;
        redisAsyncCommand(rcx, fanoutCallback, ctx, "PING");
    }
}

// called when all connections of a request returned their result
void onRequestDone(RequestCtx *ctx) {
    ctx->numRequests++;
    totalReqeusts++;

    // report...
    if (totalReqeusts % (10000 * concurrency) == 0) {
        time_t now;
        time(&now);
        time_t seconds = now - startTime;
        float rate = (float)totalReqeusts / (float)(seconds ? seconds : 1);

        printf("current rate: %.02frps\n", rate);
    }

    startRequest(ctx);
}

void getCallback(redisAsyncContext *c, void *r, void *privdata) {
    RequestCtx *ctx = privdata;

    redisReply *reply = r;

    ctx->numReplied++;
    if (ctx->numReplied == numConnections) {
        onRequestDone(ctx);
    }

    return;
}
