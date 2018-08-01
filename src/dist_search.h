#ifndef DIST_SEARCH_H
#define DIST_SEARCH_H

#include <stdlib.h>
#include "dep/rmr/rmr.h"

typedef struct {
  char *id;
  size_t idLen;
  double score;
  MRReply *fields;
  MRReply *payload;
  const char *sortKey;
  size_t sortKeyLen;
  double sortKeyNum;
} searchResult;

typedef struct {
  char *queryString;
  long long offset;
  long long limit;
  int withScores;
  int withPayload;
  int withSortby;
  int sortAscending;
  int withSortingKeys;
  int noContent;
} searchRequestCtx;

int LocalSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int FlatSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int SearchResultReducer(struct MRCtx *mc, int count, MRReply **replies);

#endif