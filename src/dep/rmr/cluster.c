#include "cluster.h"
#include "hiredis/adapters/libuv.h"

#include "dep/crc16.h"

#include <stdlib.h>

void _MRClsuter_UpdateNodes(MRCluster *cl) {
  if (cl->topo) {
    for (int sh = 0; sh < cl->topo->numShards; sh++) {
      for (int n = 0; n < cl->topo->shards[sh].numNodes; n++) {
        MRClusterNode *node = &cl->topo->shards[sh].nodes[n];
        printf("Adding node %s:%d to cluster\n", node->endpoint.host, node->endpoint.port);
        MRConnManager_Add(&cl->mgr, node->id, &node->endpoint, 0);
      }
    }
  }
}
MRCluster *MR_NewCluster(MRTopologyProvider tp, ShardFunc sf) {
  MRCluster *cl = malloc(sizeof(MRCluster));
  cl->sf = sf;
  cl->tp = tp;
  cl->topo = tp.GetTopology(tp.ctx);
  MRConnManager_Init(&cl->mgr);

  if (cl->topo) {
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
  // TODO: consider tags here
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
  printf("Executing connect CB\n");
  MRCluster *c = wrk->data;
  MRCluster_ConnectAll(c);
}

int MRCLuster_UpdateTopology(MRCluster *cl, void *ctx) {
  if (ctx) {
    cl->tp.ctx = ctx;
  }

  cl->topo = cl->tp.GetTopology(cl->tp.ctx);
  if (cl->topo) {
    _MRClsuter_UpdateNodes(cl);

    uv_work_t *wr = malloc(sizeof(uv_work_t));
    wr->data = cl;
    uv_queue_work(uv_default_loop(), wr, _clusterConnectAllCB, NULL);
  }
  return REDIS_OK;
}