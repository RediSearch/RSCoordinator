#include "cluster.h"
#include "hiredis/adapters/libuv.h"
#include <stdlib.h>

MRCluster *MR_NewCluster(MRNodeProvider np) {
  MRCluster *cl = malloc(sizeof(MRCluster));
  cl->nodes = np.GetEndpoints(np.ctx, &cl->numNodes);
  cl->conns = calloc(cl->numNodes, sizeof(redisAsyncContext *));
  cl->nodeProvider = np;
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

int MRCluster_ConnectAll(MRCluster *cl) {

  for (int i = 0; i < cl->numNodes; i++) {
    redisAsyncContext *c =
        redisAsyncConnect(cl->nodes[i].host, cl->nodes[i].port);

    if (c->err) {
      return REDIS_ERR;
    }
    c->data = cl;


    redisLibuvAttach(c, uv_default_loop());
    redisAsyncSetConnectCallback(c, connectCallback);
    redisAsyncSetDisconnectCallback(c, disconnectCallback);
    cl->conns[i] = c;
  }

  return REDIS_OK;
}


MREndpoint *MR_NewEndpoint(const char *host, int port) {

  MREndpoint *ret = malloc(sizeof(MREndpoint));
  *ret = (MREndpoint){.host = strdup(host), .port = port};
  return ret;
}

void MREndpoint_Free(MREndpoint *ep) {
    free(ep->host);
}

typedef struct {
    MREndpoint *eps;
    int num;
} __mrDummyProvider;

MREndpoint *__dummy_getNodes(void *ctx, int *num) {
    __mrDummyProvider *p = ctx;
    *num = p->num;
    return p->eps;
}


MRNodeProvider MR_NewDummyNodeProvider(int num, int startPort) {

    __mrDummyProvider *p = malloc(sizeof(__mrDummyProvider));
    p->num = num;
    p->eps = calloc(num, sizeof(MREndpoint));
    for (int i = 0; i < num; i++) {
        p->eps[i].host = strdup("localhost");
        p->eps[i].port = startPort + i;
    }

    return (MRNodeProvider){.ctx = p, .GetEndpoints = __dummy_getNodes, .SetNotifier = NULL };
}