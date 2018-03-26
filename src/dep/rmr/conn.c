#include "conn.h"
#include "hiredis/adapters/libuv.h"

#include <uv.h>
#include <signal.h>
#include <sys/param.h>

static void MRConn_ConnectCallback(const redisAsyncContext *c, int status);
static void MRConn_DisconnectCallback(const redisAsyncContext *, int);
static int MRConn_Connect(MRConn *conn);
static void MRConn_StartReconnectLoop(MRConn *conn);
static void MRConn_Free(void *ptr);
static void MRConn_Stop(MRConn *conn);
static MRConn *MR_NewConn(MREndpoint *ep);
static int MRConn_StartNewConnection(MRConn *conn);

#define RSCONN_RECONNECT_TIMEOUT 250

typedef struct {
  size_t num;
  size_t rr;  // round robin counter
  MRConn **conns;
} MRConnPool;

static MRConnPool *_MR_NewConnPool(MREndpoint *ep, size_t num) {
  MRConnPool *pool = malloc(sizeof(*pool));
  *pool = (MRConnPool){
      .num = num,
      .rr = 0,
      .conns = calloc(num, sizeof(MRConn *)),
  };

  /* Create the connection */
  for (size_t i = 0; i < num; i++) {
    pool->conns[i] = MR_NewConn(ep);
  }
  return pool;
}

static void MRConnPool_Free(void *p) {
  MRConnPool *pool = p;
  if (!pool) return;
  for (size_t i = 0; i < pool->num; i++) {
    /* We stop the connections and the disconnect callback frees them */
    MRConn_Stop(pool->conns[i]);
  }
  free(pool->conns);
  free(pool);
}

/* Get a connection from the connection pool. We select the next available connected connection with
 * a roundrobin selector */
static MRConn *MRConnPool_Get(MRConnPool *pool) {
  for (size_t i = 0; i < pool->num; i++) {

    MRConn *conn = pool->conns[pool->rr];
    // increase the round-robin counter
    pool->rr = (pool->rr + 1) % pool->num;
    if (conn->state == MRConn_Connected) {
      return conn;
    }
  }
  return NULL;
}

/* Init the connection manager */
void MRConnManager_Init(MRConnManager *mgr, int nodeConns) {
  /* Create the connection map */
  mgr->map = NewTrieMap();
  mgr->nodeConns = nodeConns;
}

/* Free the entire connection manager */
static void MRConnManager_Free(MRConnManager *mgr) {
  TrieMap_Free(mgr->map, MRConnPool_Free);
}

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id) {

  void *ptr = TrieMap_Find(mgr->map, (char *)id, strlen(id));
  if (ptr != TRIEMAP_NOTFOUND) {
    MRConnPool *pool = ptr;
    return MRConnPool_Get(pool);
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

// replace an existing coonnection pool with a new one
static void *replaceConnPool(void *oldval, void *newval) {
  if (oldval) {
    MRConnPool_Free(oldval);
  }
  return newval;
}
/* Add a node to the connection manager. Return 1 if it's been added or 0 if it hasn't */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect) {

  /* First try to see if the connection is already in the manager */
  void *ptr = TrieMap_Find(m->map, (char *)id, strlen(id));
  if (ptr != TRIEMAP_NOTFOUND) {
    MRConnPool *pool = ptr;

    MRConn *conn = pool->conns[0];
    // the node hasn't changed address, we don't need to do anything */
    if (!strcmp(conn->ep.host, ep->host) && conn->ep.port == ep->port) {
      // fprintf(stderr, "No need to switch conn pools!\n");
      return 0;
    }

    // if the node has changed, we just replace the pool with a new one automatically
  }

  MRConnPool *pool = _MR_NewConnPool(ep, m->nodeConns);
  if (connect) {
    for (size_t i = 0; i < pool->num; i++) {
      MRConn_Connect(pool->conns[i]);
    }
  }

  return TrieMap_Add(m->map, (char *)id, strlen(id), pool, replaceConnPool);
}

/**
 * Start a new connection. Returns REDISMODULE_ERR if not a new connection
 */
static int MRConn_StartNewConnection(MRConn *conn) {
  if (conn && conn->state == MRConn_Disconnected) {
    if (MRConn_Connect(conn) == REDIS_ERR) {
      MRConn_StartReconnectLoop(conn);
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
    MRConnPool *pool = p;
    if (!pool) continue;
    for (size_t i = 0; i < pool->num; i++) {
      if (MRConn_StartNewConnection(pool->conns[i]) == REDIS_OK) {
        n++;
      }
    }
  }
  TrieMapIterator_Free(it);
  return n;
}

/* Explicitly disconnect a connection and remove it from the connection pool */
int MRConnManager_Disconnect(MRConnManager *m, const char *id) {
  if (TrieMap_Delete(m->map, (char *)id, strlen(id), MRConnPool_Free)) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

/* Stop the connection and make sure it frees itself on disconnect */
static void MRConn_Stop(MRConn *conn) {
  if (conn && conn->state != MRConn_Stopping) {
    fprintf(stderr, "Stopping conn to %s:%d\n", conn->ep.host, conn->ep.port);
    conn->state = MRConn_Stopping;
    if (conn->conn) {
      redisAsyncDisconnect(conn->conn);
    }
  } else if (conn) {
    fprintf(stderr, "Conn to %s:%d already disconnecting\n", conn->ep.host, conn->ep.port);
  }
}

/* Free a connection object */
static void MRConn_Free(void *ptr) {
  MRConn *conn = ptr;
  // stop frees the connection on disconnect callback
  MRConn_Stop(conn);
}

/* Timer loop for retrying disconnected connections */
static void timerConnect(uv_timer_t *tm) {
  MRConn *conn = tm->data;

  if (MRConn_Connect(conn) == REDIS_ERR) {
    uv_timer_start(tm, timerConnect, RSCONN_RECONNECT_TIMEOUT, 0);
  } else {
    free(tm);
  }
}

/* Start the timer reconnect loop for failed connection */
static void MRConn_StartReconnectLoop(MRConn *conn) {

  conn->state = MRConn_Disconnected;
  conn->conn = NULL;
  uv_timer_t *t = malloc(sizeof(uv_timer_t));
  uv_timer_init(uv_default_loop(), t);
  t->data = conn;
  uv_timer_start(t, timerConnect, RSCONN_RECONNECT_TIMEOUT, 0);
}

static void MRConn_AuthCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRConn *conn = c->data;
  if (c->err || !r) {
    fprintf(stderr, "Error sending auth. Reconnecting...");
    conn->state = MRConn_Disconnected;
    MRConn_StartReconnectLoop(conn);
    return;
  }

  redisReply *rep = r;

  /* AUTH error */
  if (rep->type == REDIS_REPLY_ERROR) {
    fprintf(stderr, "Error authenticating: %s\n", rep->str);
    conn->state = MRConn_AuthDenied;
    /*we don't try to reconnect to failed connections */
    return;
  }

  /* Success! we are now connected! */
  // fprintf(stderr, "Connected and authenticated to %s:%d\n", conn->ep.host, conn->ep.port);
  conn->state = MRConn_Connected;
}

/* hiredis async connect callback */
static void MRConn_ConnectCallback(const redisAsyncContext *c, int status) {
  MRConn *conn = c->data;
  // fprintf(stderr, "Connect callback! status :%d\n", status);
  // if the connection is not stopped - try to reconnect
  if (status != REDIS_OK && conn->state != MRConn_Stopped) {
    fprintf(stderr, "Error on connect: %s\n", c->errstr);
    conn->state = MRConn_Disconnected;
    MRConn_StartReconnectLoop(conn);
    return;
  }

  // If this is an authenticated connection, we need to atu
  if (conn->ep.auth) {
    // the connection is now in authenticating state
    conn->state = MRConn_Authenticating;

    // if we failed to send the auth command, start a reconnect loop
    if (redisAsyncCommand((redisAsyncContext *)c, MRConn_AuthCallback, conn, "AUTH %s",
                          conn->ep.auth) == REDIS_ERR) {
      conn->state = MRConn_Disconnected;
      MRConn_StartReconnectLoop(conn);
    }

    fprintf(stderr, "Authenticating %s:%d\n", conn->ep.host, conn->ep.port);
    return;
  }
  conn->state = MRConn_Connected;
  // fprintf(stderr, "Connected %s:%d...\n", conn->ep.host, conn->ep.port);
}

static void MRConn_DisconnectCallback(const redisAsyncContext *c, int status) {

  MRConn *conn = c->data;
  // fprintf(stderr, "Disconnected from %s:%d\n", conn->ep.host, conn->ep.port);
  // MRConn_Stopped means the disconnect was initiated by us and not due to failure
  if (conn->state != MRConn_Stopping) {
    conn->state = MRConn_Disconnected;
    MRConn_StartReconnectLoop(conn);
  } else {
    conn->state = MRConn_Stopped;
    // this means we have a requested disconnect, and we remove the connection now
    // redisAsyncFree(conn->conn);
    MREndpoint_Free(&conn->ep);
    free(conn);
  }
}

static MRConn *MR_NewConn(MREndpoint *ep) {
  MRConn *conn = malloc(sizeof(MRConn));
  *conn = (MRConn){.state = MRConn_Disconnected, .conn = NULL};
  MREndpoint_Copy(&conn->ep, ep);
  return conn;
}

/* Connect to a cluster node */
static int MRConn_Connect(MRConn *conn) {

  if (conn->state == MRConn_Connected || conn->state == MRConn_Authenticating) {
    return REDIS_OK;
  }

  conn->conn = NULL;
  conn->state = MRConn_Disconnected;
  // fprintf(stderr, "Connectig to %s:%d\n", conn->ep.host, conn->ep.port);

  redisAsyncContext *c = redisAsyncConnect(conn->ep.host, conn->ep.port);
  if (c->err) {
    fprintf(stderr, "Could not connect to node %s:%d: %s\n", conn->ep.host, conn->ep.port,
            c->errstr);
    redisAsyncFree(c);
    return REDIS_ERR;
  }

  conn->conn = c;
  conn->conn->data = conn;

  redisLibuvAttach(conn->conn, uv_default_loop());
  redisAsyncSetConnectCallback(conn->conn, MRConn_ConnectCallback);
  redisAsyncSetDisconnectCallback(conn->conn, MRConn_DisconnectCallback);

  return REDIS_OK;
}
