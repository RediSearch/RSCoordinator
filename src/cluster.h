#ifndef __RMR_CLUSTER_H__
#define __RMR_CLUSTER_H__

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "command.h"
#ifndef uint
typedef unsigned int uint;
#endif
/* A single endpoint in the cluster */
typedef struct MREndpoint {
  char *host;
  int port;
  char *unixSock;
} MREndpoint;

int MREndpoint_Parse(const char *addr, MREndpoint *ep);
void MREndpoint_Free(MREndpoint *ep);

typedef struct {
  MREndpoint endpoint;
  const char *id;
  int isSelf;
  int isMaster;
  redisAsyncContext *conn;
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

// /* An interface for providing the cluster with the node list, and TODO allow node change
//  * notifications */
typedef struct {
  void *ctx;
  MRClusterTopology (*GetTopology)(void *ctx);
} MRTopologyProvider;

/* A function that tells the cluster which shard to send a command to. should return -1 if not
 * applicable */
typedef uint (*ShardFunc)(MRCommand *cmd, uint numSlots);

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct {
  MRClusterTopology topo;
  ShardFunc sf;
  MRTopologyProvider tp;
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
MRCluster *MR_NewCluster(MRTopologyProvider np, ShardFunc sharder);

uint CRC16ShardFunc(MRCommand *cmd, uint numSlots);

typedef struct {

  size_t numSlots;
  size_t numNodes;
  MRClusterNode *nodes;

} StaticTopologyProvider;

MRTopologyProvider NewStaticTopologyProvider(size_t numSlots, size_t num, ...);
#endif