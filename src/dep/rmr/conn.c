#include "conn.h"

void MRConnManager_Init(MRConnManager *mgr) {
}

void _MRConn_Free(void *ptr) {
  MRConn *conn = ptr;

  redisAsyncFree(conn->conn);
  free(ptr);
}
void MRConnManager_Free(MRConnManager *mgr) {
  MRConnManager_DisconnectAll(mgr);
  TrieMap_Free(mgr->map, _MRConn_Free);
}

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id) {

  void *ptr = TrieMap_Find(mgr->map, id, strlen(id));
  if (ptr != TRIEMAP_NOTFOUND) {
    return (MRConn *)ptr;
  }
  return NULL;
}

int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata) {

  /* Only send to connected nodes */
  if (c->state != MRConn_Connected) {
    return REDIS_ERR;
  }

  return redisAsyncCommandArgv(c->conn, fn, privdata, cmd->num, (const char **)cmd->args, NULL);
}

/* Add a node to the connection manager */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep) {
}

int MRConnManager_ConnectAll(MRConnManager *m) {
}

int MRConnManager_DisconnectAll(MRConnManager *m) {
}

int MRConnManager_Disconnect(MRConnManager *m, const char *id);
