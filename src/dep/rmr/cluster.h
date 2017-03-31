#ifndef __RMR_CLUSTER_H__
#define __RMR_CLUSTER_H__

#include "../triemap/triemap.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "conn.h"
#include "endpoint.h"
#include "command.h"
#include "node.h"

#ifndef uint
typedef unsigned int uint;
#endif

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
  MRClusterNode *myNode;
  ShardFunc sf;
  MRTopologyProvider tp;
  // map of nodes by ip:port
  MRNodeMap *nodeMap;

  // the time we last updated the topology
  // TODO: use millisecond precision time here
  time_t lastTopologyUpdate;
  // the minimum allowed interval between topology updates
  long long topologyUpdateMinInterval;
} MRCluster;

/* Define the coordination strategy of a coordination command */
typedef enum {
  MRCluster_NoCoordination,
  /* Send the coordination command to all nodes marked as coordinators */
  MRCluster_AllCoordinators,
  /* Send the command to one coordinator per physical machine (identified by its IP address) */
  MRCluster_CoordinatorPerServer,
  /* Send the command to local nodes only - i.e. nodes working on the same physical host */
  MRCluster_LocalCoordination,
} MRCoordinationStrategy;

/* Multiplex a command to all coordinators, using a specific coordination strategy */
int MRCluster_SendCoordinationCommand(MRCluster *cl, MRCoordinationStrategy strategy,
                                      MRCommand *cmd, redisCallbackFn *fn, void *privdata);

size_t MRCluster_NumHosts(MRCluster *cl);

size_t MRCluster_NumNodes(MRCluster *cl);
/* Asynchronously connect to all nodes in the cluster. This must be called before the io loop is
 * started */
int MRCluster_ConnectAll(MRCluster *cl);

// int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, int argc,
//                           const char **argv, const size_t *argvlen) {
int MRCluster_SendCommand(MRCluster *cl, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

int MRCluster_SendCommandLocal(MRCluster *cl, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

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