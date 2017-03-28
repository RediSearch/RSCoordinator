#include "cluster.h"
#include <redismodule.h>

MRClusterTopology *RedisCluster_GetTopology(void *p) {
  if (!p) return NULL;

  RedisModuleCtx *ctx = p;

  RedisModuleCallReply *r = RedisModule_Call(ctx, "CLUSTER", "c", "SLOTS");
  if (r == NULL || RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ARRAY) {
    RedisModule_Log(ctx, "error", "Error calling CLUSTER SLOTS");
    return NULL;
  }
  size_t x;
  const char *proto = RedisModule_CallReplyProto(r, &x);
  printf("%.*s\n", (int)x, proto);

  // TODO: Parse my id

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
  printf("Creating a topology of %d slots\n", len);
  MRClusterTopology *topo = calloc(1, sizeof(MRClusterTopology));

  topo->numShards = 0;
  topo->numSlots = 16384;
  topo->shards = calloc(len, sizeof(MRClusterShard));

  // iterate each nested array
  for (size_t i = 0; i < len; i++) {
    // e is slot range entry
    RedisModuleCallReply *e = RedisModule_CallReplyArrayElement(r, i);
    if (RedisModule_CallReplyLength(e) < 3) {
      printf("Invalid reply object for slot %d, type %d. len %d\n", i, RedisModule_CallReplyType(e),
             RedisModule_CallReplyLength(e));
      goto err;
    }
    // parse the start and end slots
    MRClusterShard sh;
    sh.startSlot = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(e, 0));
    sh.endSlot = RedisModule_CallReplyInteger(RedisModule_CallReplyArrayElement(e, 1));
    int numNodes = RedisModule_CallReplyLength(e) - 2;
    sh.numNodes = 0;
    sh.nodes = calloc(numNodes, sizeof(MRClusterNode));
    printf("Parsing slot %d, %d nodes", i, numNodes);
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

      sh.nodes[sh.numNodes++] = (MRClusterNode){
          .endpoint = (MREndpoint){.host = strndup(host, hostlen), .port = port, .unixSock = NULL},
          .id = strndup(id, idlen),
          .isMaster = n == 0,  // the first node is always the master
          .isSelf = 0,
      };

      printf("Added node id %s, %s:%d master? %d\n", sh.nodes[n].id, sh.nodes[n].endpoint.host,
             sh.nodes[n].endpoint.port, sh.nodes[n].isMaster);
    }
    printf("Added shard %d..%d with %d nodes\n", sh.startSlot, sh.endSlot, numNodes);
    topo->shards[topo->numShards++] = sh;
  }

  return topo;
err:
  RedisModule_Log(ctx, "error", "Error parsing cluster topology");
  MRClusterTopology_Free(topo);
  return NULL;
}

MRTopologyProvider NewRedisClusterTopologyProvider(void *ctx) {
  return (MRTopologyProvider){
      .ctx = ctx, .GetTopology = RedisCluster_GetTopology,
  };
}