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

/* A "shard" represents a slot range of the cluster, with its associated nodes. For each sharding
 * key, we select the slot based on the hash function, and then look for the shard in the cluster's
 * shard array */
typedef struct {
  uint startSlot;
  uint endSlot;
  size_t numNodes;
  MRClusterNode *nodes;
} MRClusterShard;

/* A topology is the mapping of slots to shards and nodes */
typedef struct {
  size_t numSlots;
  size_t numShards;
  MRClusterShard *shards;

} MRClusterTopology;

void MRClusterTopology_Free(MRClusterTopology *t);

void MRClusterNode_Free(MRClusterNode *n);

/* A function that tells the cluster which shard to send a command to. should return -1 if not
 * applicable */
typedef uint (*ShardFunc)(MRCommand *cmd, uint numSlots);

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct {
  /* The connection manager holds a connection to each node, indexed by node id */
  MRConnManager mgr;
  /* The latest topology of the cluster */
  MRClusterTopology *topo;
  /* the current node, detected when updating the topology */
  MRClusterNode *myNode;
  /* The sharding functino, responsible for transforming keys into slots */
  ShardFunc sf;

  /* map of nodes by ip:port */
  MRNodeMap *nodeMap;

  // the time we last updated the topology
  // TODO: use millisecond precision time here
  time_t lastTopologyUpdate;
  // the minimum allowed interval between topology updates
  long long topologyUpdateMinInterval;
} MRCluster;

/* Define the coordination strategy of a coordination command */
typedef enum {
  /* Send the coordination command to all nodes */
  MRCluster_FlatCoordination,
  /* Send the command to one coordinator per physical machine (identified by its IP address) */
  MRCluster_RemoteCoordination,
  /* Send the command to local nodes only - i.e. nodes working on the same physical host */
  MRCluster_LocalCoordination,
  /* If this is set, we only wish to talk to masters.
   * NOTE: This is a flag that should be added to the strategy along with one of the above */
  MRCluster_MastersOnly = 0x08,

} MRCoordinationStrategy;

/* Multiplex a non-sharding command to all coordinators, using a specific coordination strategy. The
 * return value is the number of nodes we managed to successfully send the command to */
int MRCluster_FanoutCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                            redisCallbackFn *fn, void *privdata);

/* Send a command to its approrpriate shard, selecting a node based on the coordination strategy.
 * Returns REDIS_OK on success, REDIS_ERR on failure. Notice that that send is asynchronous so even
 * thuogh we signal for success, the request may fail */
int MRCluster_SendCommand(MRCluster *cl, MRCoordinationStrategy strategy, MRCommand *cmd,
                          redisCallbackFn *fn, void *privdata);

/* The number of individual hosts (by IP adress) in the cluster */
size_t MRCluster_NumHosts(MRCluster *cl);

/* The number of nodes in the cluster */
size_t MRCluster_NumNodes(MRCluster *cl);

/* The number of shard instances in the cluster */
size_t MRCluster_NumShards(MRCluster *cl);

/* Asynchronously connect to all nodes in the cluster. This must be called before the io loop is
 * started */
int MRCluster_ConnectAll(MRCluster *cl);

/* Create a new cluster using a node provider */
MRCluster *MR_NewCluster(MRClusterTopology *topology, ShardFunc sharder,
                         long long minTopologyUpdateInterval);

/* Update the topology by calling the topology provider explicitly with ctx. If ctx is NULL, the
 * provider's current context is used. Otherwise, we call its function with the given context */
int MRCLuster_UpdateTopology(MRCluster *cl, MRClusterTopology *newTopology);

uint CRC16ShardFunc(MRCommand *cmd, uint numSlots);
uint CRC12ShardFunc(MRCommand *cmd, uint numSlots);


#endif