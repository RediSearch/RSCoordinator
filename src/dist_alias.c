/**
 * We maintain a normal mapping of indexes, but additionally, we
 * also have hooks which maintain a global dictionary
 */
#include "spec.h"
#include "alias.h"
#include "dist_alias.h"
#include "util/dict.h"

static dict *clAliasMap = NULL;

const char *ClusterAlias_Get(const char *alias) {
  dictEntry *e = dictFind(clAliasMap, alias);
  if (!e) {
    return NULL;
  } else {
    return e->v.val;
  }
}

static char *stripPartition(const char *s) {
  char *sdup = rm_strdup(s);
  char *openBrace = rindex(sdup, '{');
  assert(openBrace);
  *openBrace = '\0';
  return sdup;
}

static void hook_add(const char *alias, const IndexSpec *sp) {
  // strip the final '{' from each
  char *aDup = stripPartition(alias);
  char *spnameDup = stripPartition(sp->name);
  dictAdd(clAliasMap, aDup, spnameDup);
  rm_free(aDup);
}

static void hook_del(const char *alias, const IndexSpec *sp) {
  char *aDup = stripPartition(alias);
  dictEntry *ent = dictUnlink(clAliasMap, aDup);
  if (ent) {
    rm_free(ent->v.val);
    dictFreeUnlinkedEntry(clAliasMap, ent);
  }
  rm_free(aDup);
}

void ClusterAlias_Init() {
  printf("Initializing cluster alias routines...\n");
  AliasTable_g->on_add = hook_add;
  AliasTable_g->on_del = hook_del;
  clAliasMap = dictCreate(&dictTypeHeapStrings, NULL);
}