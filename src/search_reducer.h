#ifndef RSC_SEARCH_REDUCER_H
#define RSC_SEARCH_REDUCER_H
#include "dep/rmr/rmr.h"

typedef struct searchRequestCtx searchRequestCtx;
/**
 * Parse a search request; this is used by SearchResultReducer later on
 */
searchRequestCtx *rscParseRequest(RedisModuleString **argv, int argc);

/**
 * Merges a stream of one or more search replies, reordering them with appropriate
 * scores.
 */
int SearchResultReducer(struct MRCtx *mc, int count, MRReply **replies);

struct searchRequestCtx {
  char *queryString;
  long long offset;
  long long limit;
  int withScores;
  int withPayload;
  int withSortby;
  int sortAscending;
  int withSortingKeys;
  int noContent;
};
#endif