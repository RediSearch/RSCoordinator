#ifndef __QUERY_PARSER_PARSE_H__
#define __QUERY_PARSER_PARSE_H__

#include "../dep/rmr/cluster.h"
#include "parse.h"

typedef struct {
  MRClusterTopology *topology;
  char *my_id;
  int replication;
  int ok;
  char *errorMsg;
} parseCtx;

#endif  // !__QUERY_PARSER_PARSE_H__