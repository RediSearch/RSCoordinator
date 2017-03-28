#ifndef __RMR_CONN_H__
#define __RMR_CONN_H__

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "endpoint.h"
#include "../triemap/triemap.h"

typedef enum {
  MRConn_Disconnected,
  MRConn_Connected,
  MRConn_Stopped,
} MRConnState;

typedef struct {
  MREndpoint ep;
  redisAsyncContext *conn;
  MRConnState state;
} MRConn;

/* A pool indexes connections by the node id */
typedef struct { TrieMap *map; } MRConnManager;

void MRConnManager_Init(MRConnManager *mgr);

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id);

int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

/* Add a node to the connection manager */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep);

int MRConnManager_ConnectAll(MRConnManager *m);

int MRConnManager_DisconnectAll(MRConnManager *m);

int MRConnManager_Disconnect(MRConnManager *m, const char *id);

#endif