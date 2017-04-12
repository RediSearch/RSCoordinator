#ifndef _RMR_PARSER_H_
#define _RMR_PARSER_H_

#include "../cluster.h"
#include "../node.h"
#include "../endpoint.h"

typedef struct {
  int startSlot;
  int endSlot;
  MRClusterNode node;
} RLShard;

void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh);
MRClusterTopology *ParseQuery(const char *c, size_t len, char **err);
#endif