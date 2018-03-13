#include "dep/rmr/rmr.h"

int ChainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  RedisModule_ReplyWithArray(ctx, count);
  for (int i = 0; i < count; i++) {
    MR_ReplyWithMRReply(ctx, replies[i]);
  }
  // RedisModule_ReplySetArrayLength(ctx, x);
  return REDISMODULE_OK;
}

/* A reducer that just merges N arrays of strings by chaining them into one big array with no
 * duplicates */
int UniqueStringsReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  MRReply *err = NULL;

  TrieMap *dict = NewTrieMap();
  int nArrs = 0;
  // Add all the array elements into the dedup dict
  for (int i = 0; i < count; i++) {
    if (replies[i] && MRReply_Type(replies[i]) == MR_REPLY_ARRAY) {
      nArrs++;
      for (size_t j = 0; j < MRReply_Length(replies[i]); j++) {
        size_t sl = 0;
        char *s = MRReply_String(MRReply_ArrayElement(replies[i], j), &sl);
        if (s && sl) {
          TrieMap_Add(dict, s, sl, NULL, NULL);
        }
      }
    } else if (MRReply_Type(replies[i]) == MR_REPLY_ERROR && err == NULL) {
      err = replies[i];
    }
  }

  // if there are no values - either reply with an empty array or an error
  if (dict->cardinality == 0) {

    if (nArrs > 0) {
      // the arrays were empty - return an empty array
      RedisModule_ReplyWithArray(ctx, 0);
    } else {
      return RedisModule_ReplyWithError(ctx, err ? (const char *)err : "Could not perfrom query");
    }
    goto cleanup;
  }

  char *s;
  tm_len_t sl;
  void *p;
  // Iterate the dict and reply with all values
  TrieMapIterator *it = TrieMap_Iterate(dict, "", 0);
  RedisModule_ReplyWithArray(ctx, dict->cardinality);
  while (TrieMapIterator_Next(it, &s, &sl, &p)) {
    RedisModule_ReplyWithStringBuffer(ctx, s, sl);
  }

  TrieMapIterator_Free(it);

cleanup:
  TrieMap_Free(dict, NULL);

  return REDISMODULE_OK;
}
/* A reducer that just merges N arrays of the same length, selecting the first non NULL reply from
 * each */
int MergeArraysReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  int j = 0;
  int stillValid;
  do {
    // the number of still valid arrays in the response
    stillValid = 0;

    for (int i = 0; i < count; i++) {
      // if this is not an array - ignore it
      if (MRReply_Type(replies[i]) != MR_REPLY_ARRAY) continue;
      // if we've overshot the array length - ignore this one
      if (MRReply_Length(replies[i]) <= j) continue;
      // increase the number of valid replies
      stillValid++;

      // get the j element of array i
      MRReply *ele = MRReply_ArrayElement(replies[i], j);
      // if it's a valid response OR this is the last array we are scanning -
      // add this element to the merged array
      if (MRReply_Type(ele) != MR_REPLY_NIL || i + 1 == count) {
        // if this is the first reply - we need to crack open a new array reply
        if (j == 0) RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

        MR_ReplyWithMRReply(ctx, ele);
        j++;
        break;
      }
    }
  } while (stillValid > 0);

  // j 0 means we could not process a single reply element from any reply
  if (j == 0) {
    return RedisModule_ReplyWithError(ctx, "Could not process replies");
  }
  RedisModule_ReplySetArrayLength(ctx, j);

  return REDISMODULE_OK;
}

int SingleReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    return RedisModule_ReplyWithNull(ctx);
  }

  MR_ReplyWithMRReply(ctx, replies[0]);

  return REDISMODULE_OK;
}
// a reducer that expects "OK" reply for all replies, and stops at the first error and returns it
int AllOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute comand");
    return REDISMODULE_OK;
  }
  for (int i = 0; i < count; i++) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(ctx, replies[i]);
      return REDISMODULE_OK;
    }
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}