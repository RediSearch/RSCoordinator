#include "cluster.h"
#include "conn.h"
#include "uv.h"
#include <redismodule.h>

#define REDIS_CLUSTER_REFRESH_TIMEOUT 1000

typedef struct {
  MREndpoint *localEndpoint;
  MRConn *conn;
} _redisClusterTP;

void _updateTimerCB(uv_timer_t *tm);
void _updateCB(redisAsyncContext *c, void *r, void *privdata);

/* Timer loop for retrying disconnected connections */
void _updateTimerCB(uv_timer_t *tm) {
  _redisClusterTP *tp = tm->data;
  //printf("Timer update called\n");
  if (tp->conn->state == MRConn_Connected) {
    redisAsyncCommand(tp->conn->conn, _updateCB, tm, "dft.clusterrefresh");
  } else {
    uv_timer_start(tm, _updateTimerCB, REDIS_CLUSTER_REFRESH_TIMEOUT, 0);
  }
}

void _updateCB(redisAsyncContext *c, void *r, void *privdata) {
  uv_timer_t *tm = privdata;
  uv_timer_start(tm, _updateTimerCB, REDIS_CLUSTER_REFRESH_TIMEOUT, 0);
}

void _startUpdateTimer(_redisClusterTP *tp) {
  // start a refresh timer
}

int _redisCluster_init(_redisClusterTP *rc) {
  if (rc->localEndpoint == NULL) {
    return REDIS_ERR;
  }
  // create a connection to the local endpoint
  rc->conn = MR_NewConn(rc->localEndpoint);
  if (MRConn_Connect(rc->conn) == REDIS_ERR) {
    return REDIS_ERR;
  }

  uv_timer_t *t = malloc(sizeof(uv_timer_t));
  uv_timer_init(uv_default_loop(), t);
  t->data = rc;
  uv_timer_start(t, _updateTimerCB, REDIS_CLUSTER_REFRESH_TIMEOUT, 0);
  return REDIS_OK;
}

MRClusterTopology *RedisCluster_GetTopology(RedisModuleCtx *ctx) {

  const char *myId = NULL;
  RedisModuleCallReply *r = RedisModule_Call(ctx, "CLUSTER", "c", "MYID");
  if (r == NULL || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_STRING) {
    RedisModule_Log(ctx, "error", "Error calling CLUSTER MYID§");
    return NULL;
  }
  size_t idlen;
  myId = RedisModule_CallReplyStringPtr(r, &idlen);

  r = RedisModule_Call(ctx, "CLUSTER", "c", "SLOTS");
  if (r == NULL || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY) {
    RedisModule_Log(ctx, "error", "Error calling CLUSTER SLOTS");
    return NULL;
  }

  /*1) 1) (integer) 0
   2) (integer) 5460
   3) 1) "127.0.0.1"
      2) (integer) 30001
      3) "09dbe9720cda62f7865eabc5fd8857c5d2678366"
   4) 1) "127.0.0.1"
      2) (integer) 30004
      3) "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"*/

  size_t len = RedisModule_CallReplyLength(r);
  if (len < 1) {
    RedisModule_Log(ctx, "warning", "Got no slots in CLUSTER SLOTS");
    return NULL;
  }
  // printf("Creating a topology of %zd slots\n", len);
  MRClusterTopology *topo = calloc(1, sizeof(MRClusterTopology));

  topo->numShards = 0;
  topo->numSlots = 16384;
  topo->shards = calloc(len, sizeof(MRClusterShard));

  // iterate each nested array
  for (size_t i = 0; i < len; i++) {
    // e is slot range entry
    RedisModuleCallReply *e = RedisModule_CallReplyArrayElement(r, i);
    if (RedisModule_CallReplyLength(e) < 3) {
      printf("Invalid reply object for slot %zd, type %d. len %d\n", i,
             RedisModule_CallReplyType(e), (int)RedisModule_CallReplyLength(e));
      goto err;
    }
    // parse the start and end slots
    MRClusterShard sh;
    sh.startSlot = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(e, 0));
    sh.endSlot = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(e, 1));
    int numNodes = RedisModule_CallReplyLength(e) - 2;
    sh.numNodes = 0;
    sh.nodes = calloc(numNodes, sizeof(MRClusterNode));
    // printf("Parsing slot %zd, %d nodes", i, numNodes);
    // parse the nodes
    for (size_t n = 0; n < numNodes; n++) {
      RedisModuleCallReply *nd = RedisModule_CallReplyArrayElement(e, n + 2);
      // the array node must be 3 elements (future versions may add more...)
      if (RedisModule_CallReplyLength(nd) < 3) {
        goto err;
      }

      // Parse the node information (host, port, id)
      size_t hostlen, idlen;
      const char *host =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(nd, 0), &hostlen);
      long long port = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(nd, 1));
      const char *id =
          RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(nd, 2), &idlen);

      MRClusterNode node = {
          .endpoint =
              (MREndpoint){
                  .host = strndup(host, hostlen), .port = port, .auth = NULL, .unixSock = NULL},
          .id = strndup(id, idlen),
          .flags = MRNode_Coordinator,
      };
      // the first node in every shard is the master
      if (n == 0) node.flags |= MRNode_Master;

      // compare the node id to our id
      if (!strncmp(node.id, myId, idlen)) {
        // printf("Found myself %s!\n", myId);
        node.flags |= MRNode_Self;
      }
      sh.nodes[sh.numNodes++] = node;

      // printf("Added node id %s, %s:%d master? %d\n", sh.nodes[n].id, sh.nodes[n].endpoint.host,
      //        sh.nodes[n].endpoint.port, sh.nodes[n].flags & MRNode_Master);
    }
    // printf("Added shard %d..%d with %d nodes\n", sh.startSlot, sh.endSlot, numNodes);
    topo->shards[topo->numShards++] = sh;
  }

  return topo;
err:
  RedisModule_Log(ctx, "error", "Error parsing cluster topology");
  MRClusterTopology_Free(topo);
  return NULL;
}

_redisClusterTP *__redisTP = NULL;

int InitRedisTopologyUpdater(MREndpoint *currentEndpoint) {
  if (!currentEndpoint) {
    return REDIS_ERR;
  }
  __redisTP = malloc(sizeof(*__redisTP));
  __redisTP->localEndpoint = currentEndpoint;
  __redisTP->conn = NULL;
  return _redisCluster_init(__redisTP);
}