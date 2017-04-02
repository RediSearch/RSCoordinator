#include "cluster.h"
#include "hiredis/adapters/libuv.h"
#include "dep/triemap/triemap.h"
#include "dep/crc16.h"
#include "dep/rmutil/vector.h"

#include <stdlib.h>

void _MRClsuter_UpdateNodes(MRCluster *cl) {
  if (cl->topo) {

    /* Reallocate the cluster's node map */
    if (cl->nodeMap) {
      MRNodeMap_Free(cl->nodeMap);
    }
    cl->nodeMap = MR_NewNodeMap();

    /* Get all the current node ids from the connection manager.  We will remove all the nodes
     * that are in the new topology, and after the update, delete all the nodes that are in this map
     * and not in the new topology */
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
        MRNodeMap_Add(cl->nodeMap, node);

        /* Remove the node id from the current nodes ids map*/
        TrieMap_Delete(currentNodes, (char *)node->id, strlen(node->id), NULL);

        /* See if this is us - if so we need to update the cluster's host and current id */
        if (node->flags & MRNode_Self) {
          printf("This is us! %s:%d\n", node->endpoint.host, node->endpoint.port);
          cl->myNode = node;
        }
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
  cl->myNode = NULL;  // tODO: discover local ip/port
  MRConnManager_Init(&cl->mgr);

  if (cl->topo) {
    _MRClsuter_UpdateNodes(cl);
  }
  return cl;
}

/* Find the shard responsible for a given slot */
MRClusterShard *_MRCluster_FindShard(MRCluster *cl, uint slot) {
  // TODO: Switch to binary search
  for (int i = 0; i < cl->topo->numShards; i++) {
    if (cl->topo->shards[i].startSlot <= slot && cl->topo->shards[i].endSlot >= slot) {
      return &cl->topo->shards[i];
    }
  }
  return NULL;
}

/* Select a node from the shard according to the coordination strategy */
MRClusterNode *_MRClusterShard_SelectNode(MRClusterShard *sh, MRClusterNode *myNode,
                                          MRCoordinationStrategy strategy) {

  switch (strategy & ~MRCluster_MastersOnly) {

    case MRCluster_LocalCoordination:
      for (int i = 0; i < sh->numNodes; i++) {
        MRClusterNode *n = &sh->nodes[i];
        // skip slaves if this is a master only request
        if (strategy & MRCluster_MastersOnly && !(n->flags & MRNode_Master)) {
          continue;
        }
        if (MRNode_IsSameHost(n, myNode)) {
          return n;
        }
      }
      // Not found...
      return NULL;

    case MRCluster_RemoteCoordination:
      for (int i = 0; i < sh->numNodes; i++) {
        MRClusterNode *n = &sh->nodes[i];
        // skip slaves if this is a master only request
        if (strategy & MRCluster_MastersOnly && !(n->flags & MRNode_Master)) {
          continue;
        }
        if (!MRNode_IsSameHost(n, myNode)) {
          return n;
        }
      }
      // Not found...
      return NULL;

    case MRCluster_FlatCoordination:
      // if we only want masters - find the master of this shard
      if (strategy & MRCluster_MastersOnly) {
        for (int i = 0; i < sh->numNodes; i++) {
          if (sh->nodes[i].flags & MRNode_Master) {
            return &sh->nodes[i];
          }
        }
        return NULL;
      }
      // if we don't care - select a random node
      return &sh->nodes[rand() % sh->numNodes];
  }
  return NULL;
}

/* Send a single command to the right shard in the cluster, with an optoinal control over node
 * selection */
int MRCluster_SendCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                          redisCallbackFn *fn, void *privdata) {

  if (!cl->topo) {
    return REDIS_ERR;
  }

  /* Get the cluster slot from the sharder */
  uint slot = cl->sf(cmd, cl->topo->numSlots);

  /* Get the shard from the slotmap */
  MRClusterShard *sh = _MRCluster_FindShard(cl, slot);
  if (!sh) {
    return REDIS_ERR;
  }

  MRClusterNode *node = _MRClusterShard_SelectNode(sh, cl->myNode, strategy);
  if (!node) return REDIS_ERR;

  MRConn *conn = MRConn_Get(&cl->mgr, node->id);
  if (!conn) return REDIS_ERR;
  return MRConn_SendCommand(conn, cmd, fn, privdata);
}

/* Multiplex a command to all coordinators, using a specific coordination strategy. Returns the
 * number of sent commands */
int MRCluster_FanoutCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                            redisCallbackFn *fn, void *privdata) {
  if (!cl->nodeMap) {
    return 0;
  }

  MRNodeMapIterator it;
  switch (strategy) {
    case MRCluster_RemoteCoordination:
      it = MRNodeMap_IterateRandomNodePerhost(cl->nodeMap);
      break;
    case MRCluster_LocalCoordination:
      it = MRNodeMap_IterateHost(cl->nodeMap, cl->myNode->endpoint.host);
      break;
    default:
      it = MRNodeMap_IterateAll(cl->nodeMap);
  }

  int ret = 0;
  MRClusterNode *n;
  while (NULL != (n = it.Next(&it))) {
    MRConn *conn = MRConn_Get(&cl->mgr, n->id);
    printf("Sending fanout command to %s:%d\n", conn->ep.host, conn->ep.port);
    if (conn) {
      if (MRConn_SendCommand(conn, cmd, fn, privdata) != REDIS_ERR) {
        ret++;
      }
    }
  }
  return ret;
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

MRTopologyProvider NewStaticTopologyProvider(size_t numSlots, const char *auth, size_t numNodes,
                                             ...) {
  MRClusterNode *nodes = calloc(numNodes, sizeof(MRClusterNode));
  va_list ap;
  va_start(ap, numNodes);
  int n = 0;
  for (size_t i = 0; i < numNodes; i++) {
    const char *ip_port = va_arg(ap, const char *);
    if (MREndpoint_Parse(ip_port, &nodes[n].endpoint) == REDIS_OK) {
      nodes[n].endpoint.auth = auth;
      nodes[n].id = strdup(ip_port);
      nodes[n].flags = MRNode_Master | MRNode_Coordinator;
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
  // if (old) {
  //   MRClusterTopology_Free(old);
  // }
  return REDIS_OK;
}

size_t MRCluster_NumHosts(MRCluster *cl) {
  return cl->nodeMap ? MRNodeMap_NumHosts(cl->nodeMap) : 0;
}

size_t MRCluster_NumNodes(MRCluster *cl) {
  return cl->nodeMap ? MRNodeMap_NumNodes(cl->nodeMap) : 0;
}