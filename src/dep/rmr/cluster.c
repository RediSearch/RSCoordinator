#include "cluster.h"
#include "hiredis/adapters/libuv.h"
#include "dep/triemap/triemap.h"
#include "dep/crc16.h"
#include "dep/rmutil/vector.h"

#include <stdlib.h>

void *_node_replace(void *oldval, void *newval) {
  return newval;
}

void _nodemap_free(void *ptr) {
  // do not delete anything - the object is allocated elsewhere
}
void _MRCluster_NodeMapAdd(MRCluster *cl, MRClusterNode *n) {
  char addr[strlen(n->endpoint.host) + 10];
  sprintf(addr, "%s:%d", n->endpoint.host, n->endpoint.port);
  TrieMap_Add(cl->nodeMap, addr, strlen(addr), n, _node_replace);
}

/* Get all the nodes in the cluster by their IP host */
Vector *_MRCluster_GetNodesByHost(MRCluster *cl, const char *host) {
  TrieMapIterator *it = TrieMap_Iterate(cl->nodeMap, host, strlen(host));
  char *k;
  tm_len_t len;
  void *p;
  Vector *ret = NULL;
  while (TrieMapIterator_Next(it, &k, &len, &p)) {
    if (!ret) {
      ret = NewVector(MRClusterNode *, 2);
    }
    Vector_Push(ret, (MRClusterNode *)p);
  }
  return ret;
}

void _MRClsuter_UpdateNodes(MRCluster *cl) {
  if (cl->topo) {

    /* Reallocate the cluster's node map */
    if (cl->nodeMap) {
      TrieMap_Free(cl->nodeMap, _nodemap_free);
    }
    cl->nodeMap = NewTrieMap();

    /* Get all the current node ids from the connection manager.  We will remove all the nodes
     * that
     * are in the new topology, and after the update, delete all the nodes that are in this map
     * and
     * not in the new topology */
    TrieMap *currentNodes = NewTrieMap();
    TrieMapIterator *it = TrieMap_Iterate(cl->mgr.map, "", 0);
    char *k;
    tm_len_t len;
    void *p;
    while (TrieMapIterator_Next(it, &k, &len, &p)) {
      TrieMap_Add(currentNodes, k, len, NULL, NULL);
    }

    /* Walk the topology and add all nodes in it to the connection manager */
    for (int sh = 0; sh < cl->topo->numShards; sh++) {
      for (int n = 0; n < cl->topo->shards[sh].numNodes; n++) {
        MRClusterNode *node = &cl->topo->shards[sh].nodes[n];
        printf("Adding node %s:%d to cluster\n", node->endpoint.host, node->endpoint.port);
        MRConnManager_Add(&cl->mgr, node->id, &node->endpoint, 0);

        /* Add the node to the node map */
        _MRCluster_NodeMapAdd(cl, node);

        /* Remove the node id from the current node maps*/
        TrieMap_Delete(currentNodes, (char *)node->id, strlen(node->id), NULL);
      }
    }

    /* Remove all nodes that are still in the current node map and not in the new topology*/
    it = TrieMap_Iterate(currentNodes, "", 0);
    while (TrieMapIterator_Next(it, &k, &len, &p)) {
      k[len] = '\0';
      printf("Removing node %s from conn manager\n", k);
      MRConnManager_Disconnect(&cl->mgr, k);
    }

    TrieMap_Free(currentNodes, NULL);
  }
}

MRCluster *MR_NewCluster(MRTopologyProvider tp, ShardFunc sf, long long minTopologyUpdateInterval) {
  MRCluster *cl = malloc(sizeof(MRCluster));
  cl->sf = sf;
  cl->tp = tp;
  cl->topologyUpdateMinInterval = minTopologyUpdateInterval;
  cl->lastTopologyUpdate = 0;
  cl->topo = tp.GetTopology(tp.ctx);
  cl->nodeMap = NULL;
  MRConnManager_Init(&cl->mgr);

  if (cl->topo) {
    _MRCluster_CreateNodeMap(cl);
    _MRClsuter_UpdateNodes(cl);
  }
  return cl;
}

MRClusterShard *_MRCluster_FindShard(MRCluster *cl, uint slot) {
  // TODO: Switch to binary search
  for (int i = 0; i < cl->topo->numShards; i++) {
    if (cl->topo->shards[i].startSlot <= slot && cl->topo->shards[i].endSlot >= slot) {
      return &cl->topo->shards[i];
    }
  }
  return NULL;
}

/* Send a command to the right shard in the cluster */
int MRCluster_SendCommand(MRCluster *cl, MRCommand *cmd, redisCallbackFn *fn, void *privdata) {

  if (!cl->topo) {
    return REDIS_ERR;
  }
  /* Get the cluster slot from the sharder */
  uint slot = cl->sf(cmd, cl->topo->numSlots);

  /* Get the shard from the slotmap */
  MRClusterShard *sh = _MRCluster_FindShard(cl, slot);

  MRConn *conn = MRConn_Get(&cl->mgr, sh->nodes[0].id);
  if (!conn) return REDIS_ERR;
  return MRConn_SendCommand(conn, cmd, fn, privdata);
}

/* Initialize the connections to all shards */
int MRCluster_ConnectAll(MRCluster *cl) {

  return MRConnManager_ConnectAll(&cl->mgr);
}

MRClusterTopology *STP_GetTopology(void *ctx) {
  StaticTopologyProvider *stp = ctx;

  MRClusterTopology *topo = malloc(sizeof(*topo));
  topo->numShards = stp->numNodes;
  topo->numSlots = stp->numSlots;
  topo->shards = calloc(stp->numNodes, sizeof(MRClusterShard));
  size_t slotRange = topo->numSlots / topo->numShards;
  int i = 0;
  for (size_t slot = 0; slot < topo->numSlots; slot += slotRange) {
    topo->shards[i] = (MRClusterShard){
        .startSlot = slot, .endSlot = slot + slotRange - 1, .numNodes = 1,

    };
    topo->shards[i].nodes = calloc(1, sizeof(MRClusterNode)),
    topo->shards[i].nodes[0] = stp->nodes[i];

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
  char *brace = strchr(k, '{');
  size_t len = strlen(k);
  if (brace) {
    char *braceEnd = strchr(brace, '}');
    if (braceEnd) {

      len = braceEnd - brace - 1;
      k = brace + 1;
    }
  }
  uint16_t crc = crc16(k, len);
  return crc % numSlots;
}

void MRClusterTopology_Free(MRClusterTopology *t) {
  for (int s = 0; s < t->numShards; s++) {
    for (int n = 0; n < t->shards[s].numNodes; n++) {
      MRClusterNode_Free(&t->shards[s].nodes[n]);
    }
    free(t->shards[s].nodes);
  }
  free(t->shards);
  free(t);
}

size_t MRCluster_NumShards(MRCluster *cl) {
  if (cl->topo) {
    return cl->topo->numShards;
  }
  return 0;
}
void MRClusterNode_Free(MRClusterNode *n) {
  MREndpoint_Free(&n->endpoint);
  free((char *)n->id);
}

void _clusterConnectAllCB(uv_work_t *wrk) {
  // printf("Executing connect CB\n");
  MRCluster *c = wrk->data;
  MRCluster_ConnectAll(c);
}

int MRCLuster_UpdateTopology(MRCluster *cl, void *ctx) {
  if (ctx) {
    cl->tp.ctx = ctx;
  }
  // only update the topology every N seconds
  time_t now = time(NULL);
  if (cl->topo != NULL && cl->lastTopologyUpdate + cl->topologyUpdateMinInterval > now) {
    // printf("Not updating topology...\n");
    return REDIS_OK;
  }
  cl->lastTopologyUpdate = now;

  MRClusterTopology *old = cl->topo;
  cl->topo = cl->tp.GetTopology(cl->tp.ctx);
  if (cl->topo) {
    _MRClsuter_UpdateNodes(cl);

    // TODO: Fix this!
    uv_work_t *wr = malloc(sizeof(uv_work_t));
    wr->data = cl;
    uv_queue_work(uv_default_loop(), wr, _clusterConnectAllCB, NULL);
  }
  if (old) {
    MRClusterTopology_Free(old);
  }
  return REDIS_OK;
}

/* Multiplex a command to all coordinators, using a specific coordination strategy */
int MRCluster_SendCoordinationCommand(MRCluster *cl, MRCoordinationStrategy strategy,
                                      MRCommand *cmd, redisCallbackFn *fn, void *privdata) {
}