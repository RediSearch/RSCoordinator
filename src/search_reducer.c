#include "dep/rmr/rmr.h"
#include "dep/heap.h"
#include "dep/rmutil/util.h"
#include "dep/rmutil/strings.h"
#include "search_reducer.h"
#include <math.h>

typedef struct {
  char *id;
  double score;
  MRReply *fields;
  MRReply *payload;
  const char *sortKey;
  double sortKeyNum;
} searchResult;


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
    if (numReturns == 0) {
      req->noContent = 1;
    }
  }

  req->withPayload = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3) != 0;

  // Parse LIMIT argument
  RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &req->offset, &req->limit);
  if (req->limit <= 0) req->limit = 10;
  if (req->offset <= 0) req->offset = 0;

  return req;
}

int cmp_results(const void *p1, const void *p2, const void *udata) {

  const searchResult *r1 = p1, *r2 = p2;

  const searchRequestCtx *req = udata;
  int cmp = 0;
  // Compary by sorting keys
  if (r1->sortKey && r2->sortKey && req->withSortby) {
    // Sort by numeric sorting keys
    if (r1->sortKeyNum != HUGE_VAL && r2->sortKeyNum != HUGE_VAL) {
      double diff = r2->sortKeyNum - r1->sortKeyNum;
      cmp = diff < 0 ? -1 : (diff > 0 ? 1 : 0);
    } else {
      // Sort by string sort keys
      cmp = strcmp(r2->sortKey, r1->sortKey);
    }
    // in case of a tie - compare ids
    if (!cmp) cmp = strcmp(r2->id, r1->id);
    return (req->sortAscending ? -cmp : cmp);
  }

  double s1 = r1->score, s2 = r2->score;

  return s1 < s2 ? 1 : (s1 > s2 ? -1 : strcmp(r2->id, r1->id));
}

searchResult *newResult(searchResult *cached, MRReply *arr, int j, int scoreOffset,
                        int payloadOffset, int fieldsOffset, int sortKeyOffset) {
  searchResult *res = cached ? cached : malloc(sizeof(searchResult));
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;
  res->id = MRReply_String(MRReply_ArrayElement(arr, j), NULL);
  // if the id contains curly braces, get rid of them now
  char *brace = strchr(res->id, '{');
  if (brace && strchr(brace, '}')) {
    *brace = '\0';
  }
  // parse socre
  MRReply_ToDouble(MRReply_ArrayElement(arr, j + scoreOffset), &res->score);
  // get fields
  res->fields = fieldsOffset > 0 ? MRReply_ArrayElement(arr, j + fieldsOffset) : NULL;
  // get payloads
  res->payload = payloadOffset > 0 ? MRReply_ArrayElement(arr, j + payloadOffset) : NULL;

  res->sortKey =
      sortKeyOffset > 0 ? MRReply_String(MRReply_ArrayElement(arr, j + sortKeyOffset), NULL) : NULL;
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

int SearchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  searchRequestCtx *req = MRCtx_GetPrivdata(mc);

  // got no replies - this means timeout
  if (count == 0 || req->limit < 0) {
    return RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
  }

  long long total = 0;
  MRReply *lastError = NULL;
  size_t num = req->offset + req->limit;
  heap_t *pq = malloc(heap_sizeof(num));
  heap_init(pq, cmp_results, req, num);
  searchResult *cached = NULL;
  for (int i = 0; i < count; i++) {
    MRReply *arr = replies[i];
    if (!arr) continue;
    if (MRReply_Type(arr) == MR_REPLY_ERROR) {
      lastError = arr;
      continue;
    }
    if (MRReply_Type(arr) == MR_REPLY_ARRAY && MRReply_Length(arr) > 0) {
      // first element is always the total count
      total += MRReply_Integer(MRReply_ArrayElement(arr, 0));
      size_t len = MRReply_Length(arr);

      int step = 3;  // 1 for key, 1 for score, 1 for fields
      int scoreOffset = 1, fieldsOffset = 2, payloadOffset = -1, sortKeyOffset = -1;
      if (req->withPayload) {  // save an extra step for payloads
        step++;
        payloadOffset = 2;
        fieldsOffset = 3;
      }
      if (req->withSortby) {
        step++;
        sortKeyOffset = fieldsOffset++;
      }
      // nocontent - one less field, and the offset is -1 to avoid parsing it
      if (req->noContent) {
        step--;
        fieldsOffset = -1;
      }
      int jj = 0;
      // fprintf(stderr, "Step %d, scoreOffset %d, fieldsOffset %d, sortKeyOffset %d\n", step,
      //         scoreOffset, fieldsOffset, sortKeyOffset);
      for (int j = 1; j < len; j += step, jj++) {
        searchResult *res =
            newResult(cached, arr, j, scoreOffset, payloadOffset, fieldsOffset, sortKeyOffset);
        cached = NULL;
        // fprintf(stderr, "Response %d result %d Reply docId %s score: %f sortkey %f\n", i, j,
        //         res->id, res->score, res->sortKeyNum);

        if (heap_count(pq) < heap_size(pq)) {
          // printf("Offering result score %f\n", res->score);
          heap_offerx(pq, res);

        } else {
          searchResult *smallest = heap_peek(pq);
          int c = cmp_results(res, smallest, req);
          if (c < 0) {
            smallest = heap_poll(pq);
            heap_offerx(pq, res);
            cached = smallest;
          } else {
            // If the result is lower than the last result in the heap - we can stop now
            cached = res;
            break;
          }
        }
      }
    }
  }
  if (cached) free(cached);
  // If we didn't get any results and we got an error - return it.
  // If some shards returned results and some errors - we prefer to show the results we got an not
  // return an error. This might change in the future
  if (total == 0 && lastError != NULL) {
    MR_ReplyWithMRReply(ctx, lastError);
    searchRequestCtx_Free(req);

    return REDISMODULE_OK;
  }

  // Reverse the top N results
  size_t qlen = heap_count(pq);

  // // If we didn't get enough results - we return nothing
  // if (qlen <= req->offset) {
  //   qlen = 0;
  // }

  size_t pos = qlen;
  searchResult *results[qlen];
  while (pos) {
    results[--pos] = heap_poll(pq);
  }
  heap_free(pq);

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int len = 1;
  RedisModule_ReplyWithLongLong(ctx, total);
  for (pos = req->offset; pos < qlen && pos < num; pos++) {
    searchResult *res = results[pos];
    RedisModule_ReplyWithStringBuffer(ctx, res->id, strlen(res->id));
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
        RedisModule_ReplyWithStringBuffer(ctx, res->sortKey, strlen(res->sortKey));
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
  for (pos = 0; pos < qlen; pos++) {
    free(results[pos]);
  }

  searchRequestCtx_Free(req);

  return REDISMODULE_OK;
}

