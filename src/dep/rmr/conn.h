#ifndef __RMR_CONN_H__
#define __RMR_CONN_H__

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "endpoint.h"
#include "command.h"

#include "../triemap/triemap.h"

#define MR_CONN_POOL_SIZE 2

/* The state of the connection */
typedef enum {
  /* initial state - new connection or disconnected connection due to error */
  MRConn_Disconnected,

  /* Connection is trying to connect */
  MRConn_Connecting,

  /* Connected but still needs authentication */
  MRConn_Authenticating,

  /* Auth failed state */
  MRConn_AuthDenied,

  /* Connected, authenticated and active */
  MRConn_Connected,

  /* Stopping due to user request */
  MRConn_Stopping,

  /* Stopped due to user request */
  MRConn_Stopped,
} MRConnState;

typedef struct {
  MREndpoint ep;
  redisAsyncContext *conn;
  MRConnState state;
} MRConn;

/* A pool indexes connections by the node id */
typedef struct {
  TrieMap *map;
  int nodeConns;
} MRConnManager;

void MRConnManager_Init(MRConnManager *mgr, int nodeConns);

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id);

int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

MRConn *MR_NewConn(MREndpoint *ep);

/* Connect the connection and start a reconnect loop if it failed */
int MRConn_Connect(MRConn *conn);

void MRConn_Stop(MRConn *conn);
/* Add a node to the connection manager */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect);

/* Connect all nodes to their destinations */
int MRConnManager_ConnectAll(MRConnManager *m);

/* Disconnect a node */
int MRConnManager_Disconnect(MRConnManager *m, const char *id);

#endif