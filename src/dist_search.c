#include "dist_search.h"
#include "redismodule.h"
#include "search_cluster.h"
#include "dep/rmr/rmr.h"
#include "util/heap.h"
#include "util/minmax.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include <float.h>
#include <math.h>

void searchRequestCtx_Free(searchRequestCtx *r) {
  free(r->queryString);
  free(r);
}

searchRequestCtx *rscParseRequest(RedisModuleString **argv, int argc) {
  /* A search request must have at least 3 args */
  if (argc < 3) {
    return NULL;
  }

  searchRequestCtx *req = malloc(sizeof(searchRequestCtx));
  req->queryString = strdup(RedisModule_StringPtrLen(argv[2], NULL));
  req->limit = 10;
  req->offset = 0;
  // marks the user set WITHSCORES. internally it's always set
  req->withScores = RMUtil_ArgExists("WITHSCORES", argv, argc, 3) != 0;

  // Parse SORTBY ... ASC
  int sortByIndex = RMUtil_ArgIndex("SORTBY", argv, argc);
  req->withSortby = sortByIndex > 2;
  req->sortAscending = 1;
  if (req->withSortby && sortByIndex + 2 < argc) {
    if (RMUtil_StringEqualsCaseC(argv[sortByIndex + 2], "DESC")) {
      req->sortAscending = 0;
    }
  }

  req->withSortingKeys = RMUtil_ArgExists("WITHSORTKEYS", argv, argc, 3) != 0;
  // fprintf(stderr, "Sortby: %d, asc: %d withsort: %d\n", req->withSortby, req->sortAscending,
  //         req->withSortingKeys);

  // Detect "NOCONTENT"
  req->noContent = RMUtil_ArgExists("NOCONTENT", argv, argc, 3) != 0;

  // if RETURN exists - make sure we don't have RETURN 0
  if (!req->noContent && RMUtil_ArgExists("RETURN", argv, argc, 3)) {
    long long numReturns = -1;
    RMUtil_ParseArgsAfter("RETURN", argv, argc, "l", &numReturns);
    // RETURN 0 equals NOCONTENT
    if (numReturns <= 0) {
      req->noContent = 1;
    }
  }

  req->withPayload = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3) != 0;

  // Parse LIMIT argument
  RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &req->offset, &req->limit);
  if (req->limit < 0 || req->offset < 0) {
    free(req);
    return NULL;
  }
  if (req->limit == 0) {
    req->limit = 10;
  }

  return req;
}

static int cmpStrings(const char *s1, size_t l1, const char *s2, size_t l2) {
  int cmp = memcmp(s1, s2, Min(l1, l2));
  if (l1 == l2) {
    // if the strings are the same length, just return the result of strcmp
    return cmp;
  }

  // if the strings are identical but the lengths aren't, return the longer string
  if (cmp == 0) {
    return l1 > l2 ? 1 : -1;
  } else {  // the strings are lexically different, just return that
    return cmp;
  }
}

static int cmp_results(const void *p1, const void *p2, const void *udata) {

  const searchResult *r1 = p1, *r2 = p2;
  const searchRequestCtx *req = udata;
  // Compary by sorting keys
  if ((r1->sortKey || r2->sortKey) && req->withSortby) {
    int cmp = 0;
    // Sort by numeric sorting keys
    if (r1->sortKeyNum != HUGE_VAL && r2->sortKeyNum != HUGE_VAL) {
      double diff = r2->sortKeyNum - r1->sortKeyNum;
      cmp = diff < 0 ? -1 : (diff > 0 ? 1 : 0);
    } else if (r1->sortKey && r2->sortKey) {

      // Sort by string sort keys
      cmp = cmpStrings(r2->sortKey, r2->sortKeyLen, r1->sortKey, r1->sortKeyLen);
      // printf("Using sortKey!! <N=%lu> %.*s vs <N=%lu> %.*s. Result=%d\n", r2->sortKeyLen,
      //        (int)r2->sortKeyLen, r2->sortKey, r1->sortKeyLen, (int)r1->sortKeyLen, r1->sortKey,
      //        cmp);
    } else {
      // If at least one of these has a sort key
      cmp = r2->sortKey ? 1 : -1;
    }
    // in case of a tie - compare ids
    if (!cmp) {
      // printf("It's a tie! Comparing <N=%lu> %.*s vs <N=%lu> %.*s\n", r2->idLen, (int)r2->idLen,
      //        r2->id, r1->idLen, (int)r1->idLen, r1->id);
      cmp = cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);
    }
    return (req->sortAscending ? -cmp : cmp);
  }

  double s1 = r1->score, s2 = r2->score;
  // printf("Scores: %lf vs %lf. WithSortBy: %d. SK1=%p. SK2=%p\n", s1, s2, req->withSortby,
  //        r1->sortKey, r2->sortKey);
  if (s1 < s2) {
    return 1;
  } else if (s1 > s2) {
    return -1;
  } else {
    // printf("Scores are tied. Will compare ID Strings instead\n");
    int rv = cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);
    // printf("ID Strings: Comparing <N=%lu> %.*s vs <N=%lu> %.*s => %d\n", r2->idLen,
    // (int)r2->idLen,
    //        r2->id, r1->idLen, (int)r1->idLen, r1->id, rv);
    return rv;
  }
}

static searchResult *newResult(searchResult *cached, MRReply *arr, int j, int scoreOffset,
                               int payloadOffset, int fieldsOffset, int sortKeyOffset) {
  searchResult *res = cached ? cached : malloc(sizeof(searchResult));
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;
  res->id = MRReply_String(MRReply_ArrayElement(arr, j), &res->idLen);
  // if the id contains curly braces, get rid of them now
  if (res->id) {
    MRKey mk;
    MRKey_Parse(&mk, res->id, res->idLen);
    res->idLen = mk.baseLen;
    res->id = mk.base;
    if (mk.shardLen) {
      res->id[res->idLen] = '\0';
    }
  } else {  // this usually means an invalid result
    return res;
  }
  // parse socre
  MRReply_ToDouble(MRReply_ArrayElement(arr, j + scoreOffset), &res->score);
  // get fields
  res->fields = fieldsOffset > 0 ? MRReply_ArrayElement(arr, j + fieldsOffset) : NULL;
  // get payloads
  res->payload = payloadOffset > 0 ? MRReply_ArrayElement(arr, j + payloadOffset) : NULL;
  if (sortKeyOffset > 0) {
    res->sortKey = MRReply_String(MRReply_ArrayElement(arr, j + sortKeyOffset), &res->sortKeyLen);
  } else {
    res->sortKey = NULL;
  }
  if (res->sortKey) {
    if (res->sortKey[0] == '#') {
      char *eptr;
      double d = strtod(res->sortKey + 1, &eptr);
      if (eptr != res->sortKey + 1 && *eptr == 0) {
        res->sortKeyNum = d;
      }
    }
    // fprintf(stderr, "Sort key string '%s', num '%f\n", res->sortKey, res->sortKeyNum);
  }
  return res;
}

typedef struct {
  MRReply *lastError;
  searchResult *cachedResult;
  searchRequestCtx *searchCtx;
  heap_t *pq;
  size_t totalReplies;
} searchReducerCtx;

typedef struct {
  int step;  // offset for next reply
  int score;
  int firstField;
  int payload;
  int sortKey;
} searchReplyOffsets;

static void getReplyOffsets(const searchRequestCtx *ctx, searchReplyOffsets *offsets) {
  offsets->step = 3;  // 1 for key, 1 for score, 1 for fields
  offsets->score = 1;

  offsets->firstField = 2;
  offsets->payload = -1;
  offsets->sortKey = -1;

  if (ctx->withPayload) {  // save an extra step for payloads
    offsets->step++;
    offsets->payload = 2;
    offsets->firstField = 3;
  }
  if (ctx->withSortby || ctx->withSortingKeys) {
    offsets->step++;
    offsets->sortKey = offsets->firstField++;
  }
  // nocontent - one less field, and the offset is -1 to avoid parsing it
  if (ctx->noContent) {
    offsets->step--;
    offsets->firstField = -1;
  }
}

static void processSearchReply(MRReply *arr, searchReducerCtx *rCtx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    rCtx->lastError = arr;
    return;
  }
  if (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0) {
    // Empty reply??
    return;
  }

  // first element is always the total count
  rCtx->totalReplies += MRReply_Integer(MRReply_ArrayElement(arr, 0));
  size_t len = MRReply_Length(arr);
  searchReplyOffsets offsets = {0};
  getReplyOffsets(rCtx->searchCtx, &offsets);

  // fprintf(stderr, "Step %d, scoreOffset %d, fieldsOffset %d, sortKeyOffset %d\n", step,
  //         scoreOffset, fieldsOffset, sortKeyOffset);
  for (int j = 1; j < len; j += offsets.step) {
    searchResult *res = newResult(rCtx->cachedResult, arr, j, offsets.score, offsets.payload,
                                  offsets.firstField, offsets.sortKey);
    if (!res || !res->id) {
      // invalid result - usually means something is off with the response, and we should just
      // quit this response
      rCtx->cachedResult = res;
      break;
    } else {
      rCtx->cachedResult = NULL;
    }

    // fprintf(stderr, "Response %d result %d Reply docId %s score: %f sortkey %f\n", i, j,
    //         res->id, res->score, res->sortKeyNum);

    // TODO: minmax_heap?
    if (heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
      // printf("Offering result score %f\n", res->score);
      heap_offerx(rCtx->pq, res);

    } else {
      searchResult *smallest = heap_peek(rCtx->pq);
      int c = cmp_results(res, smallest, rCtx->searchCtx);
      if (c < 0) {
        smallest = heap_poll(rCtx->pq);
        heap_offerx(rCtx->pq, res);
        rCtx->cachedResult = smallest;
      } else {
        rCtx->cachedResult = res;
        if (rCtx->searchCtx->withSortby) {
          // If the result is lower than the last result in the heap,
          // AND there is a user-defined sort order - we can stop now
          break;
        }
      }
    }
  }
}

static void sendSearchResults(RedisModuleCtx *ctx, searchReducerCtx *rCtx) {
  // Reverse the top N results
  searchRequestCtx *req = rCtx->searchCtx;

  // Number of results to actually return
  size_t num = req->limit + req->offset;

  size_t qlen = heap_count(rCtx->pq);
  size_t pos = qlen;

  // Load the results from the heap into a sorted array. Free the items in
  // the heap one-by-one so that we don't have to go through them again
  searchResult *results[qlen];
  while (pos) {
    results[--pos] = heap_poll(rCtx->pq);
  }
  heap_free(rCtx->pq);
  rCtx->pq = NULL;

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithLongLong(ctx, rCtx->totalReplies);
  size_t len = 1;

  for (pos = rCtx->searchCtx->offset; pos < qlen && pos < num; pos++) {
    searchResult *res = results[pos];
    RedisModule_ReplyWithStringBuffer(ctx, res->id, res->idLen);
    len++;
    if (req->withScores) {
      RedisModule_ReplyWithDouble(ctx, res->score);
      len++;
    }
    if (req->withPayload) {

      MR_ReplyWithMRReply(ctx, res->payload);
      len++;
    }
    if (req->withSortingKeys && req->withSortby) {
      len++;
      if (res->sortKey) {
        RedisModule_ReplyWithStringBuffer(ctx, res->sortKey, res->sortKeyLen);
      } else {
        RedisModule_ReplyWithNull(ctx);
      }
    }
    if (!req->noContent) {
      MR_ReplyWithMRReply(ctx, res->fields);
      len++;
    }
  }
  RedisModule_ReplySetArrayLength(ctx, len);

  // Free the sorted results
  for (pos = 0; pos < qlen; pos++) {
    free(results[pos]);
  }
}

int SearchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  searchRequestCtx *req = MRCtx_GetPrivdata(mc);
  searchReducerCtx rCtx = {NULL};

  // got no replies - this means timeout
  if (count == 0 || req->limit < 0) {
    return RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
  }

  size_t num = req->offset + req->limit;
  rCtx.pq = malloc(heap_sizeof(num));
  heap_init(rCtx.pq, cmp_results, req, num);

  rCtx.searchCtx = req;

  for (int i = 0; i < count; i++) {
    processSearchReply(replies[i], &rCtx);
  }
  if (rCtx.cachedResult) {
    free(rCtx.cachedResult);
  }
  // If we didn't get any results and we got an error - return it.
  // If some shards returned results and some errors - we prefer to show the results we got an not
  // return an error. This might change in the future
  if (rCtx.totalReplies == 0 && rCtx.lastError != NULL) {
    MR_ReplyWithMRReply(ctx, rCtx.lastError);
  } else {
    sendSearchResults(ctx, &rCtx);
  }

  if (rCtx.pq) {
    searchResult *res;
    while ((res = heap_poll(rCtx.pq))) {
      free(res);
    }
    heap_free(rCtx.pq);
  }

  searchRequestCtx_Free(req);
  return REDISMODULE_OK;
}

int SearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);
  // MR_UpdateTopology(ctx);

  // If this a one-node cluster, we revert to a simple, flat one level coordination
  // if (MR_NumHosts() < 2) {
  //   return LocalSearchCommandHandler(ctx, argv, argc);
  // }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_ReplaceArg(&cmd, 0, "FT.LSEARCH", sizeof("FT.LSEARCH") - 1);

  searchRequestCtx *req = rscParseRequest(argv, argc);
  if (!req) {
    return RedisModule_ReplyWithError(ctx, "Invalid search request");
  }
  // Internally we must have WITHSCORES set, even if the usr didn't set it
  if (!req->withScores) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSCORES");
  }
  // MRCommand_Print(&cmd);

  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  MR_SetCoordinationStrategy(mrctx, MRCluster_RemoteCoordination | MRCluster_MastersOnly);
  MR_Fanout(mrctx, SearchResultReducer, cmd);

  return REDIS_OK;
}

int LocalSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // MR_UpdateTopology(ctx);
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  searchRequestCtx *req = rscParseRequest(argv, argc);
  if (!req) {
    return RedisModule_ReplyWithError(ctx, "Invalid search request");
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  if (!req->withScores) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSCORES");
  }
  if (!req->withSortingKeys && req->withSortby) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSORTKEYS");
  }

  // replace the LIMIT {offset} {limit} with LIMIT 0 {limit}, because we need all top N to merge
  int limitIndex = RMUtil_ArgExists("LIMIT", argv, argc, 3);
  if (limitIndex && req->limit > 0 && limitIndex < argc - 2) {
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0", 1);
  }

  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "_FT.SEARCH", sizeof("_FT.SEARCH") - 1);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_LocalCoordination | MRCluster_MastersOnly);

  MR_Map(mrctx, SearchResultReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int FlatSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // MR_UpdateTopology(ctx);
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  searchRequestCtx *req = rscParseRequest(argv, argc);
  if (!req) {
    return RedisModule_ReplyWithError(ctx, "Invalid search request");
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  if (!req->withScores) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSCORES");
  }

  if (!req->withSortingKeys && req->withSortby) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSORTKEYS");
    // req->withSortingKeys = 1;
  }

  // replace the LIMIT {offset} {limit} with LIMIT 0 {limit}, because we need all top N to merge
  int limitIndex = RMUtil_ArgExists("LIMIT", argv, argc, 3);
  if (limitIndex && req->limit > 0 && limitIndex < argc - 2) {
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0", 1);
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", req->limit + req->offset);
    MRCommand_ReplaceArg(&cmd, limitIndex + 2, buf, strlen(buf));
  }

  // Tag the InKeys arguments
  int inKeysPos = RMUtil_ArgIndex("INKEYS", argv, argc);

  if (inKeysPos > 2) {
    long long numFilteredIds = 0;
    // Get the number of INKEYS args
    RMUtil_ParseArgsAfter("INKEYS", &argv[inKeysPos], argc - inKeysPos, "l", &numFilteredIds);
    // If we won't overflow - tag each key
    if (numFilteredIds > 0 && numFilteredIds + inKeysPos + 1 < argc) {
      inKeysPos += 2;  // the start of the actual keys
      for (int x = inKeysPos; x < inKeysPos + numFilteredIds && x < argc; x++) {
        SearchCluster_RewriteCommandArg(GetSearchCluster(), &cmd, x, x);
      }
    }
  }
  // MRCommand_Print(&cmd);

  /* Replace our own FT command with _FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "_FT.SEARCH", sizeof("_FT.SEARCH") - 1);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_FlatCoordination);

  MR_Map(mrctx, SearchResultReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}
