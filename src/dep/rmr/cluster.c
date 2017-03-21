#include "cluster.h"
#include "hiredis/adapters/libuv.h"

#include "dep/crc16.h"

#include <stdlib.h>

MRCluster *MR_NewCluster(MRTopologyProvider tp, ShardFunc sf) {
  MRCluster *cl = malloc(sizeof(MRCluster));
  cl->sf = sf;
  cl->tp = tp;
  cl->topo = tp.GetTopology(tp.ctx);
  return cl;
}

void __timerConnect(uv_timer_t *tm) {
  MRClusterNode *n = tm->data;
  if (MRNode_Connect(n) == REDIS_ERR) {
    uv_timer_start(tm, __timerConnect, 100, 0);
  } else {

    free(tm);
  }
}

void _MRNode_StartReconnectLoop(MRClusterNode *n) {
  n->connected = 0;
  n->conn = NULL;
  uv_timer_t *t = malloc(sizeof(uv_timer_t));
  uv_timer_init(uv_default_loop(), t);
  t->data = n;
  uv_timer_start(t, __timerConnect, 100, 0);
}

void _MRNode_ConnectCallback(const redisAsyncContext *c, int status) {
  MRClusterNode *n = c->data;
  if (status != REDIS_OK) {
    // printf("Error on connect: %s\n", c->errstr);
    _MRNode_StartReconnectLoop(n);
    return;
  }

  n->connected = 1;
  printf("Connected %s...\n", n->id);
}

void _MRNode_DisconnectCallback(const redisAsyncContext *c, int status) {

  MRClusterNode *n = c->data;
  n->connected = 0;
  printf("Disconnected from %s\n", n->id);
  _MRNode_StartReconnectLoop(n);
}

/* Connect to a cluster node */
int MRNode_Connect(MRClusterNode *n) {

  n->conn = NULL;
  n->connected = 0;
  redisAsyncContext *c = redisAsyncConnect(n->endpoint.host, n->endpoint.port);
  if (c->err) {
    redisAsyncFree(c);
    return REDIS_ERR;
  }

  n->conn = c;
  n->conn->data = n;

  redisLibuvAttach(n->conn, uv_default_loop());
  redisAsyncSetConnectCallback(n->conn, _MRNode_ConnectCallback);
  redisAsyncSetDisconnectCallback(n->conn, _MRNode_DisconnectCallback);

  return REDIS_OK;
}

MRClusterShard *_MRCluster_FindShard(MRCluster *cl, uint slot) {
  // TODO: Switch to binary search
  for (int i = 0; i < cl->topo.numShards; i++) {
    if (cl->topo.shards[i].startSlot <= slot && cl->topo.shards[i].endSlot >= slot) {
      return &cl->topo.shards[i];
    }
  }
  return NULL;
}

/* Send a command to the right shard in the cluster */
int MRCluster_SendCommand(MRCluster *cl, MRCommand *cmd, redisCallbackFn *fn, void *privdata) {

  /* Get the cluster slot from the sharder */
  uint slot = cl->sf(cmd, cl->topo.numSlots);

  /* Get the shard from the slotmap */
  MRClusterShard *sh = _MRCluster_FindShard(cl, slot);

  /* We couldn't find a shard for this slot. Not command for you */
  if (!sh || sh->nodes[0].conn == NULL || sh->nodes[0].connected == 0) {
    return REDIS_ERR;
  }

  return redisAsyncCommandArgv(sh->nodes[0].conn, fn, privdata, cmd->num, (const char **)cmd->args,
                               NULL);
}

void __connectLoop(uv_work_t *wr) {

  MRClusterNode *n = wr->data;
  printf("Connecting to %s\n", n->id);
  if (MRNode_Connect(n) != REDIS_OK) {
    printf("error connecting to %s:%d\n", n->endpoint.host, n->endpoint.port);
    wr->data = n;

    wr = malloc(sizeof(uv_work_t));
    wr->data = n;
    printf("Enqueueing reconnect\n");
    uv_queue_work(uv_default_loop(), wr, __connectLoop, NULL);
    // uv_queue_work(uv_default_loop(), wr, __connectLoop, NULL);
    return;
  }
  printf("Connected to %s ok!\n", n->id);
}
/* Initialize the connections to all shards */
int MRCluster_ConnectAll(MRCluster *cl) {

  for (int i = 0; i < cl->topo.numShards; i++) {

    MRClusterShard *sh = &cl->topo.shards[i];
    for (int j = 0; j < sh->numNodes; j++) {

      if (MRNode_Connect(&sh->nodes[j]) != REDIS_OK) {

        uv_work_t *wr = malloc(sizeof(uv_work_t));
        wr->data = &sh->nodes[j];
        uv_queue_work(uv_default_loop(), wr, __connectLoop, NULL);
      }
    }
  }
  return REDIS_OK;
}

int MREndpoint_Parse(const char *addr, MREndpoint *ep) {

  ep->host = NULL;
  ep->unixSock = NULL;
  char *colon = strchr(addr, ':');
  if (!colon || colon == addr) {
    return REDIS_ERR;
  }

  ep->host = strndup(addr, colon - addr);
  ep->port = atoi(colon + 1);

  if (ep->port <= 0 || ep->port > 0xFFFF) {
    return REDIS_ERR;
  }
  return REDIS_OK;
}

void MREndpoint_Free(MREndpoint *ep) {
  if (ep->host) {
    free(ep->host);
    ep->host = NULL;
  }
  if (ep->unixSock) {
    free(ep->unixSock);
    ep->unixSock = NULL;
  }
}

MRClusterTopology STP_GetTopology(void *ctx) {
  StaticTopologyProvider *stp = ctx;
  MRClusterTopology topo;
  topo.numShards = stp->numNodes;
  topo.numSlots = stp->numSlots;
  topo.shards = calloc(stp->numNodes, sizeof(MRClusterShard));
  size_t slotRange = topo.numSlots / topo.numShards;
  int i = 0;
  for (size_t slot = 0; slot < topo.numSlots; slot += slotRange) {
    topo.shards[i] = (MRClusterShard){
        .startSlot = slot, .endSlot = slot + slotRange - 1, .numNodes = 1,

    };
    topo.shards[i].nodes = calloc(1, sizeof(MRClusterNode)),
    topo.shards[i].nodes[0] = stp->nodes[i];
    i++;
  }

  return topo;
}

MRTopologyProvider NewStaticTopologyProvider(size_t numSlots, size_t numNodes, ...) {
  MRClusterNode *nodes = calloc(numNodes, sizeof(MRClusterNode));
  va_list ap;
  va_start(ap, numNodes);
  int n = 0;
  for (size_t i = 0; i < numNodes; i++) {
    const char *ip_port = va_arg(ap, const char *);
    if (MREndpoint_Parse(ip_port, &nodes[n].endpoint) == REDIS_OK) {
      nodes[n].id = strdup(ip_port);
      nodes[n].isMaster = 1;
      n++;
    }
  }
  va_end(ap);

  StaticTopologyProvider *prov = malloc(sizeof(StaticTopologyProvider));
  prov->nodes = nodes;
  prov->numNodes = n;
  prov->numSlots = numSlots;
  return (MRTopologyProvider){
      .ctx = prov, .GetTopology = STP_GetTopology,
  };
}

uint CRC16ShardFunc(MRCommand *cmd, uint numSlots) {

  const char *k = cmd->args[MRCommand_GetShardingKey(cmd)];

  uint16_t crc = crc16(k, strlen(k));
  return crc % numSlots;
}