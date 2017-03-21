#include <stdlib.h>
#include <string.h>

#include "redismodule.h"
#include "dep/rmr/rmr.h"
#include "dep/rmr/hiredis/async.h"
#include "dep/rmr/reply.h"
#include "dep/rmutil/util.h"
#include "fnv.h"
#include "dep/heap.h"
#include "search_cluster.h"
#include "config.h"

/* A reducer that just chains the replies from a map request */
int chainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  RedisModule_ReplyWithArray(ctx, count);
  for (int i = 0; i < count; i++) {
    MR_ReplyWithMRReply(ctx, replies[i]);
  }
  return REDISMODULE_OK;
}

typedef struct {
  MRReply *id;
  double score;
  MRReply *fields;
  MRReply *payload;
} searchResult;

typedef struct {
  char *queryString;
  long long offset;
  long long limit;
  int withScores;
  int withPayload;
  int noContent;
} searchRequestCtx;

void searchRequestCtx_Free(searchRequestCtx *r) {
  free(r->queryString);
  free(r);
}

searchRequestCtx *parseRequest(RedisModuleString **argv, int argc) {
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

  // Detect "NOCONTENT"
  req->noContent = RMUtil_ArgExists("NOCONTENT", argv, argc, 3) != 0;
  req->withPayload = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3) != 0;

  // Parse LIMIT argument
  RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &req->offset, &req->limit);
  if (req->limit <= 0) req->limit = 10;
  if (req->offset <= 0) req->offset = 0;

  return req;
}

int cmp_results(const void *p1, const void *p2, const void *udata) {

  const searchResult *r1 = p1, *r2 = p2;

  double s1 = r1->score, s2 = r2->score;

  return s1 < s2 ? 1 : (s1 > s2 ? -1 : 0);
}

searchResult *newResult(MRReply *arr, int j, int scoreOffset, int payloadOffset, int fieldsOffset) {
  searchResult *res = malloc(sizeof(searchResult));
  res->id = MRReply_ArrayElement(arr, j);
  // parse socre
  MRReply_ToDouble(MRReply_ArrayElement(arr, j + scoreOffset), &res->score);
  // get fields
  res->fields = fieldsOffset > 0 ? MRReply_ArrayElement(arr, j + fieldsOffset) : NULL;
  // get payloads
  res->payload = payloadOffset > 0 ? MRReply_ArrayElement(arr, j + payloadOffset) : NULL;
  return res;
}

int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  searchRequestCtx *req = MRCtx_GetPrivdata(mc);

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

      int step = 3;  // 1 for key, 1 for score, 1 for fields
      int scoreOffset = 1, fieldsOffset = 2, payloadOffset = -1;
      if (req->withPayload) {  // save an extra step for payloads
        step++;
        payloadOffset = 2;
        fieldsOffset = 3;
      }
      // nocontent - one less field, and the offset is -1 to avoid parsing it
      if (req->noContent) {
        step--;
        fieldsOffset = -1;
      }

      for (int j = 1; j < len; j += step) {
        searchResult *res = newResult(arr, j, scoreOffset, payloadOffset, fieldsOffset);

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
    len++;
    if (req->withScores) {
      RedisModule_ReplyWithDouble(ctx, res->score);
      len++;
    }
    if (req->withPayload) {
      
      MR_ReplyWithMRReply(ctx, res->payload);
      len++;
    }
    if (!req->noContent) {
      MR_ReplyWithMRReply(ctx, res->fields);
      len++;
    }
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
  SearchCluster sc = NewSearchCluster(clusterConfig.numPartitions,
                                      NewSimplePartitioner(clusterConfig.numPartitions));

  SearchCluster_RewriteCommand(&sc, &cmd, 2);
  MRCommand_Print(&cmd);
  // MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd, 1);
  MR_MapSingle(MR_CreateCtx(ctx, NULL), chainReplyReducer, cmd);

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

  SearchCluster sc = NewSearchCluster(clusterConfig.numPartitions,
                                      NewSimplePartitioner(clusterConfig.numPartitions));

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd, 1);
  MR_Map(MR_CreateCtx(ctx, NULL), chainReplyReducer, cg);

  // MRCommand_Free(&cmd);
  // cg.Free(cg.ctx);

  // MR_Fanout(MR_CreateCtx(ctx), sumReducer, cmd);

  return REDISMODULE_OK;
}

int SearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  searchRequestCtx *req = parseRequest(argv, argc);
  if (!req) {
    return RedisModule_ReplyWithError(ctx, "Invalid search request");
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  if (!req->withScores) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSCORES");
  }
  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "FT.SEARCH");
  free(cmd.args[0]);
  cmd.args[0] = strdup("FT.SEARCH");

  SearchCluster sc = NewSearchCluster(clusterConfig.numPartitions,
                                      NewSimplePartitioner(clusterConfig.numPartitions));

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd, 1);

  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);

  MR_Map(mrctx, searchResultReducer, cg);

  // MRCommand_Free(&cmd);
  // cg.Free(cg.ctx);

  // MR_Fanout(MR_CreateCtx(ctx), sumReducer, cmd);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, "rmr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /* Init cluster configs */
  if (argc > 0) {
    if (ParseConfig(&clusterConfig, argv, argc) == REDISMODULE_ERR) {
      // printf("Could not parse module config\n");
      RedisModule_Log(ctx, "warning", "Could not parse module config");
      return REDISMODULE_ERR;
    }
  } else {
    RedisModule_Log(ctx, "warning", "No module config, reverting to default settings");
    clusterConfig = DEFAULT_CLUSTER_CONFIG;
  }

  RedisModule_Log(ctx, "notice", "Cluster configuration: %d partitions, type: %s",
                  clusterConfig.numPartitions, clusterConfig.clusterType);

  MRTopologyProvider tp = NewStaticTopologyProvider(4096, 2, "localhost:6375", "localhost:6376",
                                                    "localhost:6377", "localhost:6378");
  MRCluster *cl = MR_NewCluster(tp, CRC16ShardFunc);
  MR_Init(cl);
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