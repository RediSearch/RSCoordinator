#include "cluster.h"
#include "hiredis/adapters/libuv.h"
#include <dep/triemap/triemap.h>
#include "crc16.h"
#include "crc12.h"
#include <dep/rmutil/vector.h>

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
    TrieMapIterator_Free(it);

    /* Walk the topology and add all nodes in it to the connection manager */
    for (int sh = 0; sh < cl->topo->numShards; sh++) {
      for (int n = 0; n < cl->topo->shards[sh].numNodes; n++) {
        MRClusterNode *node = &cl->topo->shards[sh].nodes[n];
        // printf("Adding node %s:%d to cluster\n", node->endpoint.host, node->endpoint.port);
        MRConnManager_Add(&cl->mgr, node->id, &node->endpoint, 0);

        /* Add the node to the node map */
        MRNodeMap_Add(cl->nodeMap, node);

        /* Remove the node id from the current nodes ids map*/
        TrieMap_Delete(currentNodes, (char *)node->id, strlen(node->id), NULL);

        /* See if this is us - if so we need to update the cluster's host and current id */
        if (node->flags & MRNode_Self) {
          cl->myNode = node;
        }
      }
    }

    /* Remove all nodes that are still in the current node map and not in the new topology*/
    it = TrieMap_Iterate(currentNodes, "", 0);
    while (TrieMapIterator_Next(it, &k, &len, &p)) {
      k[len] = '\0';
      // printf("Removing node %s from conn manager\n", k);
      MRConnManager_Disconnect(&cl->mgr, k);
    }
    TrieMapIterator_Free(it);
    TrieMap_Free(currentNodes, NULL);
  }
}

MRCluster *MR_NewCluster(MRClusterTopology *initialTopolgy, ShardFunc sf,
                         long long minTopologyUpdateInterval) {
  MRCluster *cl = malloc(sizeof(MRCluster));
  cl->sf = sf;
  cl->topologyUpdateMinInterval = minTopologyUpdateInterval;
  cl->lastTopologyUpdate = 0;
  cl->topo = initialTopolgy;
  cl->nodeMap = NULL;
  cl->myNode = NULL;  // tODO: discover local ip/port
  MRConnManager_Init(&cl->mgr, 4);

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
  switch (strategy & ~(MRCluster_MastersOnly)) {
    case MRCluster_RemoteCoordination:
      it = MRNodeMap_IterateRandomNodePerhost(cl->nodeMap, cl->myNode);
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
    if ((strategy & MRCluster_MastersOnly) && !(n->flags & MRNode_Master)) {
      continue;
    }
    MRConn *conn = MRConn_Get(&cl->mgr, n->id);
    // printf("Sending fanout command to %s:%d\n", conn->ep.host, conn->ep.port);
    if (conn) {
      if (MRConn_SendCommand(conn, cmd, fn, privdata) != REDIS_ERR) {
        ret++;
      }
    }
  }
  MRNodeMapIterator_Free(&it);

  return ret;
}

/* Initialize the connections to all shards */
int MRCluster_ConnectAll(MRCluster *cl) {

  return MRConnManager_ConnectAll(&cl->mgr);
}

char *_MRGetShardKey(MRCommand *cmd, size_t *len) {
  int pos = MRCommand_GetShardingKey(cmd);
  if (pos < 0) {
    return NULL;
  }
  char *k = cmd->args[MRCommand_GetShardingKey(cmd)];
  char *brace = strchr(k, '{');
  *len = strlen(k);
  if (brace) {
    char *braceEnd = strchr(brace, '}');
    if (braceEnd) {

      *len = braceEnd - brace - 1;
      k = brace + 1;
    }
  }

  return k;
}

uint CRC16ShardFunc(MRCommand *cmd, uint numSlots) {

  size_t len;

  char *k = _MRGetShardKey(cmd, &len);
  if (!k) return 0;
  uint16_t crc = crc16(k, len);
  return crc % numSlots;
}

uint CRC12ShardFunc(MRCommand *cmd, uint numSlots) {
  size_t len;

  char *k = _MRGetShardKey(cmd, &len);
  if (!k) return 0;
  uint16_t crc = crc12(k, len);
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

int MRCLuster_UpdateTopology(MRCluster *cl, MRClusterTopology *newTopo) {

  // only update the topology every N seconds
  time_t now = time(NULL);
  if (cl->topo != NULL && cl->lastTopologyUpdate + cl->topologyUpdateMinInterval > now) {
    // printf("Not updating topology...\n");
    return REDIS_OK;
  }
  cl->lastTopologyUpdate = now;

  MRClusterTopology *old = cl->topo;
  cl->topo = newTopo;
  if (cl->topo) {
    _MRClsuter_UpdateNodes(cl);

    MRCluster_ConnectAll(cl);
  }
  if (old) {
    MRClusterTopology_Free(old);
  }
  return REDIS_OK;
}

size_t MRCluster_NumHosts(MRCluster *cl) {
  return cl->nodeMap ? MRNodeMap_NumHosts(cl->nodeMap) : 0;
}

size_t MRCluster_NumNodes(MRCluster *cl) {
  return cl->nodeMap ? MRNodeMap_NumNodes(cl->nodeMap) : 0;
}

MRClusterShard MR_NewClusterShard(int startSlot, int endSlot, size_t capNodes) {
  MRClusterShard ret = (MRClusterShard){
      .startSlot = startSlot,
      .endSlot = endSlot,
      .capNodes = capNodes,
      .numNodes = 0,
      .nodes = calloc(capNodes, sizeof(MRClusterNode)),
  };
  return ret;
}

void MRClusterShard_AddNode(MRClusterShard *sh, MRClusterNode *n) {
  if (sh->capNodes == sh->numNodes) {
    sh->capNodes += 1;
    sh->nodes = realloc(sh->nodes, sh->capNodes * sizeof(MRClusterNode));
  }
  sh->nodes[sh->numNodes++] = *n;
}

MRClusterTopology *MR_NewTopology(size_t numShards, size_t numSlots) {
  MRClusterTopology *topo = calloc(1, sizeof(*topo));
  topo->capShards = numShards;
  topo->numShards = 0;
  topo->numSlots = numSlots;
  topo->shards = calloc(topo->capShards, sizeof(MRClusterShard));
  return topo;
}

void MRClusterTopology_AddShard(MRClusterTopology *topo, MRClusterShard *sh) {
  if (topo->capShards == topo->numShards) {
    topo->capShards++;
    topo->shards = realloc(topo->shards, topo->capShards * sizeof(MRClusterShard));
  }
  topo->shards[topo->numShards++] = *sh;
}