#include "conn.h"
#include "hiredis/adapters/libuv.h"

#include <uv.h>
#include <signal.h>
#include <sys/param.h>

void _MRConn_ConnectCallback(const redisAsyncContext *c, int status);
void _MRConn_DisconnectCallback(const redisAsyncContext *, int);
int _MRConn_Connect(MRConn *conn);
void _MRConn_StartReconnectLoop(MRConn *conn);

void _MRConn_Free(void *ptr);

#define RSCONN_RECONNECT_TIMEOUT 250

/* Init the connection manager */
void MRConnManager_Init(MRConnManager *mgr) {
  /* Create the connection map */
  mgr->map = NewTrieMap();
}

/* Free the entire connection manager */
void MRConnManager_Free(MRConnManager *mgr) {
  TrieMap_Free(mgr->map, _MRConn_Free);
}

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id) {

  void *ptr = TrieMap_Find(mgr->map, (char *)id, strlen(id));
  if (ptr != TRIEMAP_NOTFOUND) {
    return (MRConn *)ptr;
  }
  return NULL;
}

/* Send a command to the connection */
int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata) {

  /* Only send to connected nodes */
  if (c->state != MRConn_Connected) {
    return REDIS_ERR;
  }

  return redisAsyncCommandArgv(c->conn, fn, privdata, cmd->num, (const char **)cmd->args, NULL);
}

/* Add a node to the connection manager. Return 1 if it's been added or 0 if it hasn't */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect) {

  /* First try to see if the connection is already in the manager */
  MRConn *conn = MRConn_Get(m, id);
  if (conn) {

    // if the address has changed - we stop the connection and we'll re-initiate it later
    if (strcmp(conn->ep.host, ep->host) || conn->ep.port != ep->port) {
      MRConn_Stop(conn);
    } else {
      // TODO: What if the connection's detils changed?
      return 0;
    }
  }

  conn = MR_NewConn(ep);
  if (!conn) {
    return 0;
  }

  int rc = TrieMap_Add(m->map, (char *)id, strlen(id), conn, NULL);
  if (connect) {
    _MRConn_Connect(conn);
  }
  return rc;
}

int MRConn_Connect(MRConn *conn) {
  if (conn && conn->state == MRConn_Disconnected) {
    if (_MRConn_Connect(conn) == REDIS_ERR) {
      _MRConn_StartReconnectLoop(conn);
    }
    return REDIS_OK;
  }
  return REDIS_ERR;
}

/* Connect all connections in the manager. Return the number of connections we successfully
 * started.
 * If we cannot connect, we initialize a retry loop */
int MRConnManager_ConnectAll(MRConnManager *m) {

  int n = 0;
  TrieMapIterator *it = TrieMap_Iterate(m->map, "", 0);
  char *key;
  tm_len_t len;
  void *p;
  while (TrieMapIterator_Next(it, &key, &len, &p)) {
    MRConn *conn = p;
    if (MRConn_Connect(conn) == REDIS_OK) {
      n++;
    }
  }
  return n;
}

/* Explicitly disconnect a connection and remove it from the connection pool */
int MRConnManager_Disconnect(MRConnManager *m, const char *id) {

  if (TrieMap_Delete(m->map, (char *)id, strlen(id), _MRConn_Free)) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

/* Stop the connection and make sure it frees itself on disconnect */
void MRConn_Stop(MRConn *conn) {
  conn->state = MRConn_Stopping;
  redisAsyncDisconnect(conn->conn);
}

/* Free a connection object */
void _MRConn_Free(void *ptr) {
  MRConn *conn = ptr;
  // stop frees the connection on disconnect callback
  MRConn_Stop(conn);
}

/* Timer loop for retrying disconnected connections */
void __timerConnect(uv_timer_t *tm) {
  MRConn *conn = tm->data;

  if (_MRConn_Connect(conn) == REDIS_ERR) {
    uv_timer_start(tm, __timerConnect, RSCONN_RECONNECT_TIMEOUT, 0);
  } else {
    free(tm);
  }
}

/* Start the timer reconnect loop for failed connection */
void _MRConn_StartReconnectLoop(MRConn *conn) {

  conn->state = MRConn_Disconnected;
  conn->conn = NULL;
  uv_timer_t *t = malloc(sizeof(uv_timer_t));
  uv_timer_init(uv_default_loop(), t);
  t->data = conn;
  uv_timer_start(t, __timerConnect, RSCONN_RECONNECT_TIMEOUT, 0);
}

void _MRConn_AuthCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRConn *conn = c->data;
  if (c->err || !r) {
    printf("Error sending auth. Reconnecting...");
    conn->state = MRConn_Disconnected;
    _MRConn_StartReconnectLoop(conn);
  }

  redisReply *rep = r;

  /* AUTH error */
  if (rep->type == REDIS_REPLY_ERROR) {
    printf("Error authenticating: %s\n", rep->str);
    conn->state = MRConn_AuthDenied;
    /*we don't try to reconnect to failed connections */
    return;
  }

  /* Success! we are now connected! */
  printf("Connected and authenticated to %s:%d\n", conn->ep.host, conn->ep.port);
  conn->state = MRConn_Connected;
}

/* hiredis async connect callback */
void _MRConn_ConnectCallback(const redisAsyncContext *c, int status) {
  MRConn *conn = c->data;
  // printf("Connect callback! status :%d\n", status);
  // if the connection is not stopped - try to reconnect
  if (status != REDIS_OK && conn->state != MRConn_Stopped) {
    printf("Error on connect: %s\n", c->errstr);
    conn->state = MRConn_Disconnected;
    _MRConn_StartReconnectLoop(conn);
    return;
  }

  // If this is an authenticated connection, we need to atu
  if (conn->ep.auth) {
    // the connection is now in authenticating state
    conn->state = MRConn_Authenticating;

    // if we failed to send the auth command, start a reconnect loop
    if (redisAsyncCommand((redisAsyncContext *)c, _MRConn_AuthCallback, conn, "AUTH %s",
                          conn->ep.auth) == REDIS_ERR) {
      conn->state = MRConn_Disconnected;
      _MRConn_StartReconnectLoop(conn);
    }

    printf("Authenticating %s\n", conn->ep.host, conn->ep.port);
    return;
  }
  conn->state = MRConn_Connected;
  printf("Connected %s:%d...\n", conn->ep.host, conn->ep.port);
}

void _MRConn_DisconnectCallback(const redisAsyncContext *c, int status) {

  MRConn *conn = c->data;
  // printf("Disconnected from %s:%d\n", conn->ep.host, conn->ep.port);
  // MRConn_Stopped means the disconnect was initiated by us and not due to failure
  if (conn->state != MRConn_Stopping) {
    conn->state = MRConn_Disconnected;
    _MRConn_StartReconnectLoop(conn);
  } else {
    conn->state = MRConn_Stopped;
    // this means we have a requested disconnect, and we remove the connection now
    redisAsyncFree(conn->conn);
    free(conn);
  }
}

MRConn *MR_NewConn(MREndpoint *ep) {
  MRConn *conn = malloc(sizeof(MRConn));
  *conn = (MRConn){.ep = *ep, .state = MRConn_Disconnected, .conn = NULL};
  return conn;
}

/* Connect to a cluster node */
int _MRConn_Connect(MRConn *conn) {

  if (conn->state == MRConn_Connected || conn->state == MRConn_Authenticating) {
    return REDIS_OK;
  }

  conn->conn = NULL;
  conn->state = MRConn_Disconnected;
  // printf("Connectig to %s:%d\n", conn->ep.host, conn->ep.port);

  redisAsyncContext *c = redisAsyncConnect(conn->ep.host, conn->ep.port);
  if (c->err) {
    printf("Could not connect to node %s:%d: %s\n", conn->ep.host, conn->ep.port, c->errstr);
    redisAsyncFree(c);
    return REDIS_ERR;
  }

  conn->conn = c;
  conn->conn->data = conn;

  redisLibuvAttach(conn->conn, uv_default_loop());
  redisAsyncSetConnectCallback(conn->conn, _MRConn_ConnectCallback);
  redisAsyncSetDisconnectCallback(conn->conn, _MRConn_DisconnectCallback);

  return REDIS_OK;
}
