/**
 * We maintain a normal mapping of indexes, but additionally, we
 * also have hooks which maintain a global dictionary
 */
#include <map>
#include <string>
#include "spec.h"
#include "alias.h"
#include "dist_alias.h"

static std::map<std::string, std::string> clAliasMap;

const char *ClusterAlias_Get(const char *alias) {
  auto r = clAliasMap.find(alias);
  if (r == clAliasMap.end()) {
    return NULL;
  } else {
    return r->second.c_str();
  }
}

static void stripPartition(std::string &s) {
  s = s.substr(0, s.size() - 5);
}

static void hook_add(const char *alias, const IndexSpec *sp) {
  // strip the final '{' from each
  std::string alias_s(alias);
  std::string sp_s(sp->name);

  stripPartition(alias_s);
  stripPartition(sp_s);
  clAliasMap[alias_s] = sp_s;
}

static void hook_del(const char *alias, const IndexSpec *sp) {
  std::string alias_s(alias);
  stripPartition(alias_s);
  clAliasMap.erase(alias_s);
}

void ClusterAlias_Init() {
  AliasTable_g->on_add = hook_add;
  AliasTable_g->on_del = hook_del;
}