#ifndef __RMR_CLUSTER_H__
#define __RMR_CLUSTER_H__

#include "hiredis/hiredis.h"
#include "hiredis/async.h"

/* A single endpoint in the cluster */
typedef struct MREndpoint {
  char *host;
  int port;
} MREndpoint;

/* An interface for providing the cluster with the node list, and TODO allow node change notifications */
typedef struct  {
    void *ctx;
    MREndpoint *(*GetEndpoints)(void *ctx, int *num);
    void (*SetNotifier)(void (*callback)(void *ctx, MREndpoint *eps, int num));
} MRNodeProvider;

/* A cluster has nodes and connections that can be used by the engine to send requests */
typedef struct MRCluster {
  int numNodes;
  MREndpoint *nodes;
  redisAsyncContext **conns;
  MRNodeProvider nodeProvider;
} MRCluster;
 
typedef void (*MRClusterNotifyCallback)(MREndpoint **, int);

/* Create a new Endpoint object */
MREndpoint *MR_NewEndpoint(const char *host, int port);

/* Asynchronously connect to all nodes in the cluster. This must be called before the io loop is started */
int MRCluster_ConnectAll(MRCluster *cl);

/* Create a new cluster using a node provider */
struct MRCluster *MR_NewCluster(MRNodeProvider np); 

// for now we use a really silly node provider
MRNodeProvider MR_NewDummyNodeProvider(int num, int startPort);

#endif