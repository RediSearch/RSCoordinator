#ifndef __RMR_CONN_H__
#define __RMR_CONN_H__

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "endpoint.h"
#include "command.h"

#include "../triemap/triemap.h"

#define MR_CONN_POOL_SIZE 4

/* The state of the connection */
typedef enum {
  /* initial state - new connection or disconnected connection due to error */
  MRConn_Disconnected,

  /* Connection is trying to connect */
  MRConn_Connecting,

  /* Reconnecting from failed connection attempt */
  MRConn_Reconnecting,

  /* Connected but still needs authentication */
  MRConn_Authenticating,

  /* Auth failed state */
  MRConn_AuthDenied,

  /* Connected, authenticated and active */
  MRConn_Connected,

  /* Connection should be freed */
  MRConn_Freeing
} MRConnState;

static inline const char *MRConnState_Str(MRConnState state) {
  switch (state) {
    case MRConn_Disconnected:
      return "Disconnected";
    case MRConn_Connecting:
      return "Connecting";
    case MRConn_Reconnecting:
      return "Reconnecting";
    case MRConn_Authenticating:
      return "Authenticating";
    case MRConn_AuthDenied:
      return "Auth Denied";
    case MRConn_Connected:
      return "Connected";
    case MRConn_Freeing:
      return "Freeing";
    default:
      return "<UNKNOWN STATE (CRASHES AHEAD!!!!)";
  }
}

typedef struct {
  MREndpoint ep;
  redisAsyncContext *conn;
  MRConnState state;
  void *timer;
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

/* Add a node to the connection manager */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect);

/* Connect all nodes to their destinations */
int MRConnManager_ConnectAll(MRConnManager *m);

/* Disconnect a node */
int MRConnManager_Disconnect(MRConnManager *m, const char *id);

#endif