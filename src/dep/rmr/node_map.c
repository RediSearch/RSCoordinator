#include <stdlib.h>
#include "node.h"
#include "dep/triemap/triemap.h"
#include "dep/triemap/triemap.h"

typedef struct MRNodeMap {
  TrieMap *nodes;
  TrieMap *hosts;
} MRNodeMap;

void *_node_replace(void *oldval, void *newval) {
  return newval;
}

void _nodemap_free(void *ptr) {
  // do not delete anything - the object is allocated elsewhere
}

void MRNodeMap_Free(MRNodeMap *m) {
  TrieMap_Free(m->hosts, NULL);
  TrieMap_Free(m->nodes, _nodemap_free);
  free(m);
}

MRNodeMap *MR_NewNodeMap() {
  MRNodeMap *m = malloc(sizeof(*m));
  m->hosts = NewTrieMap();
  m->nodes = NewTrieMap();
  return m;
}

void MRNodeMap_Add(MRNodeMap *m, MRClusterNode *n) {

  TrieMap_Add(m->hosts, n->endpoint.host, strlen(n->endpoint.host), NULL, NULL);

  char addr[strlen(n->endpoint.host) + 10];
  sprintf(addr, "%s:%d", n->endpoint.host, n->endpoint.port);
  TrieMap_Add(m->nodes, addr, strlen(addr), n, _node_replace);
}

MRClusterNode *MRNodeMap_RandomNode(MRNodeMap *m) {
  char *k;
  tm_len_t len;
  void *p;
  MRNodeMap *ret = NULL;
  if (TrieMap_RandomKey(m->nodes, &k, &len, &p)) {
    ret = TrieMap_Find(m->nodes, k, len);
    free(k);
  }
  return ret;
}