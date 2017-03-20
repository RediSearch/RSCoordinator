#include <stdlib.h>
#include <string.h>

#include "redismodule.h"
#include "rmr.h"
#include "hiredis/async.h"
#include "reply.h"
#include "fnv.h"
#include "dep/heap.h"
#include "search_cluster.h"
/* A reducer that just chains the replies from a map request */
int chainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetPrivdata(mc);

  RedisModule_ReplyWithArray(ctx, count);
  for (int i = 0; i < count; i++) {
    printf("Reply: %p\n", replies[i]);
    MR_ReplyWithMRReply(ctx, replies[i]);
  }
  return REDISMODULE_OK;
}

typedef struct {
  MRReply *id;
  double score;
  MRReply *fields;
} searchResult;

int cmp_results(const void *p1, const void *p2, const void *udata) {

  const searchResult *r1 = p1, *r2 = p2;

  double s1 = r1->score, s2 = r2->score;

  return s1 < s2 ? 1 : (s1 > s2 ? -1 : 0);
}

searchResult *newResult(MRReply *arr, int j) {
  searchResult *res = malloc(sizeof(searchResult));
  res->id = MRReply_ArrayElement(arr, j);
  res->fields = MRReply_ArrayElement(arr, j + 2);
  MRReply_ToDouble(MRReply_ArrayElement(arr, j + 1), &res->score);
  return res;
}

int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetPrivdata(mc);
  printf("Count: %d\n", count);
  int N = 0;
  long long total = 0;
  double minScore = 0;
  heap_t *pq = malloc(heap_sizeof(10));
  heap_init(pq, cmp_results, NULL, 10);
  for (int i = 0; i < count; i++) {
    MRReply *arr = replies[i];
    if (MRReply_Type(arr) == MR_REPLY_ARRAY && MRReply_Length(arr) > 0) {
      // first element is always the total count
      total += MRReply_Integer(MRReply_ArrayElement(arr, 0));
      size_t len = MRReply_Length(arr);
      for (int j = 1; j < len; j += 3) {
        searchResult *res = newResult(arr, j);

        printf("Reply score: %f, minScore: %f\n", res->score, minScore);

        if (heap_count(pq) < heap_size(pq)) {
          printf("Offering result score %f\n", res->score);
          heap_offerx(pq, res);

        } else if (res->score > minScore) {
          printf("Heap full!\n");
          searchResult *smallest = heap_poll(pq);
          heap_offerx(pq, res);
          free(smallest);
        } else {
          free(res);
        }
        if (res->score < minScore) {
          minScore = res->score;
        }
      }
    }
  }
  // TODO: Inverse this
  printf("Total: %d\n", total);
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int len = 1;
  RedisModule_ReplyWithLongLong(ctx, total);
  while (heap_count(pq)) {

    searchResult *res = heap_poll(pq);
    MR_ReplyWithMRReply(ctx, res->id);
    RedisModule_ReplyWithDouble(ctx, res->score);
    MR_ReplyWithMRReply(ctx, res->fields);
    len += 3;
  }
  RedisModule_ReplySetArrayLength(ctx, len);

  return REDISMODULE_OK;
}

/* DFT.ADD {index} ... */
int SingleShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own DFT command with FT. command */
  char *tmp = cmd.args[0];
  cmd.args[0] = strdup(cmd.args[0] + 1);
  printf("Turning %s into %s\n", tmp, cmd.args[0]);
  free(tmp);
  cmd.keyPos = 1;
  SearchCluster sc = NewSearchCluster(20, NewSimplePartitioner(20));

  SearchCluster_RewriteCommand(&sc, &cmd, 2);
  MRCommand_Print(&cmd);
  // MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd, 1);
  MR_MapSingle(MR_CreateCtx(ctx), chainReplyReducer, cmd);

  MRCommand_Free(&cmd);

  // MR_Fanout(MR_CreateCtx(ctx), sumReducer, cmd);

  return REDISMODULE_OK;
}

int FanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own DFT command with FT. command */
  char *tmp = cmd.args[0];
  cmd.args[0] = strdup(cmd.args[0] + 1);
  printf("Turning %s into %s\n", tmp, cmd.args[0]);
  free(tmp);

  SearchCluster sc = NewSearchCluster(20, NewSimplePartitioner(20));

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd, 1);
  MR_Map(MR_CreateCtx(ctx), chainReplyReducer, cg);

  // MRCommand_Free(&cmd);
  // cg.Free(cg.ctx);

  // MR_Fanout(MR_CreateCtx(ctx), sumReducer, cmd);

  return REDISMODULE_OK;
}

int SearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own DFT command with FT. command */
  free(cmd.args[0]);
  cmd.args[0] = strdup("FT.SEARCH");

  SearchCluster sc = NewSearchCluster(20, NewSimplePartitioner(20));

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd, 1);
  MR_Map(MR_CreateCtx(ctx), searchResultReducer, cg);

  // MRCommand_Free(&cmd);
  // cg.Free(cg.ctx);

  // MR_Fanout(MR_CreateCtx(ctx), sumReducer, cmd);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

  MRTopologyProvider tp = NewStaticTopologyProvider(4096, 2, "localhost:6375", "localhost:6376",
                                                    "localhost:6377", "localhost:6378");
  MRCluster *cl = MR_NewCluster(tp, CRC16ShardFunc);
  MR_Init(cl);

  if (RedisModule_Init(ctx, "rmr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // register index type

  if (RedisModule_CreateCommand(ctx, "dft.add", SingleShardCommandHandler, "write", 0, 0, 0) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "dft.create", FanoutCommandHandler, "write", 0, 0, 0) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "dft.search", SearchCommandHandler, "readonly", 0, 0, 0) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}