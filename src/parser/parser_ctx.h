#ifndef __QUERY_PARSER_PARSE_H__
#define __QUERY_PARSER_PARSE_H__

#include "topology.h"

typedef struct {
  MRClusterTopology *topology;
  char *my_id;
  int replication;
} parseCtx;

typedef struct {
  int startSlot;
  int endSlot;
  MRClusterNode node;
} RLShard;

void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh);

#endif  // !__QUERY_PARSER_PARSE_H__