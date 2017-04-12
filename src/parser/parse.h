#ifndef _RMR_PARSER_H_
#define _RMR_PARSER_H_

#include "../dep/rmr/cluster.h"
#include "../dep/rmr/node.h"
#include "../dep/rmr/endpoint.h"

typedef struct {
  int startSlot;
  int endSlot;
  MRClusterNode node;
} RLShard;

void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh);
MRClusterTopology *ParseQuery(const char *c, size_t len, char **err);
#endif