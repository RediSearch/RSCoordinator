#ifndef __RMR_CLUSTER_H__
#define __RMR_CLUSTER_H__

#include "../triemap/triemap.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "conn.h"
#include "endpoint.h"
#include "command.h"

#ifndef uint
typedef unsigned int uint;
#endif

typedef struct {
  MREndpoint endpoint;
  const char *id;
  int isSelf;
  int isMaster;
} MRClusterNode;

typedef struct {
  uint startSlot;
  uint endSlot;
  size_t numNodes;
  MRClusterNode *nodes;
} MRClusterShard;

typedef struct {
  size_t numSlots;
  size_t numShards;
  MRClusterShard *shards;

} MRClusterTopology;

void MRClusterTopology_Free(MRClusterTopology *t);
void MRClusterNode_Free(MRClusterNode *n);

// /* An interface for providing the cluster with the node list, and TODO allow node change
//  * notifications */
typedef struct {
  void *ctx;
  MRClusterTopology *(*GetTopology)(void *ctx);
} MRTopologyProvider;

/* A function that tells the cluster which shard to send a command to. should return -1 if not
 * applicable */
typedef uint (*ShardFunc)(MRCommand *cmd, uint numSlots);

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct {
  MRConnManager mgr;
  MRClusterTopology *topo;
  ShardFunc sf;
  MRTopologyProvider tp;

  // the time we last updated the topology
  time_t lastTopologyUpdate;
  // the minimum allowed interval between topology updates
  long long topologyUpdateMinInterval;
} MRCluster;

// /* Create a new Endpoint object */
// MRNode *MR_NewNode(const char *host, int port, const char *unixsock);
/* Free an MRendpoint object */
void MRNode_Free(MRClusterNode *n);

int MRNode_Connect(MRClusterNode *n);

/* Asynchronously connect to all nodes in the cluster. This must be called before the io loop is
 * started */
int MRCluster_ConnectAll(MRCluster *cl);

// int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, int argc,
//                           const char **argv, const size_t *argvlen) {
int MRCluster_SendCommand(MRCluster *cl, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

/* Create a new cluster using a node provider */
MRCluster *MR_NewCluster(MRTopologyProvider np, ShardFunc sharder,
                         long long minTopologyUpdateInterval);

int MRCLuster_UpdateTopology(MRCluster *cl, void *ctx);

size_t MRCluster_NumShards(MRCluster *cl);
uint CRC16ShardFunc(MRCommand *cmd, uint numSlots);

typedef struct {

  size_t numSlots;
  size_t numNodes;
  MRClusterNode *nodes;

} StaticTopologyProvider;

MRTopologyProvider NewStaticTopologyProvider(size_t numSlots, size_t num, ...);
#endif