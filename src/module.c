#include <stdlib.h>
#include <string.h>

#include "redismodule.h"
#include "dep/rmr/rmr.h"
#include "dep/rmr/hiredis/async.h"
#include "dep/rmr/reply.h"
#include "dep/rmutil/util.h"
#include "crc16_tags.h"
#include "crc12_tags.h"
#include "dep/rmr/redis_cluster.h"
#include "dep/rmr/redise.h"
#include "fnv.h"
#include "dep/heap.h"
#include "search_cluster.h"
#include "periodic.h"
#include "config.h"

SearchCluster __searchCluster;
RSPeriodicCommand *__periodicGC = NULL;

/* A reducer that just chains the replies from a map request */
int chainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  RedisModule_ReplyWithArray(ctx, count);
  for (int i = 0; i < count; i++) {
    MR_ReplyWithMRReply(ctx, replies[i]);
  }
  return REDISMODULE_OK;
}

int singleReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithNull(ctx);
  }

  MR_ReplyWithMRReply(ctx, replies[0]);

  return REDISMODULE_OK;
}
// a reducer that expects "OK" reply for all replies, and stops at the first error and returns it
int allOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
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

typedef struct {
  char *id;
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
  return res;
}

int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  searchRequestCtx *req = MRCtx_GetPrivdata(mc);

  // got no replies - this means timeout
  if (count == 0 || req->limit < 0) {
    RedisModule_ReplyWithError(ctx, "Could not send query to cluter");
  }

  long long total = 0;
  double minScore = 0;
  heap_t *pq = malloc(heap_sizeof(req->offset + req->limit));
  heap_init(pq, cmp_results, NULL, req->offset + req->limit);
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

        // printf("Reply score: %f, minScore: %f\n", res->score, minScore);

        if (heap_count(pq) < heap_size(pq)) {
          // printf("Offering result score %f\n", res->score);
          heap_offerx(pq, res);

        } else if (res->score > minScore) {
          searchResult *smallest = heap_poll(pq);
          heap_offerx(pq, res);
          minScore = smallest->score;
          free(smallest);
        } else {
          free(res);
        }
      }
    }
  }

  // Reverse the top N results
  size_t qlen = heap_count(pq);
  size_t pos = qlen;
  searchResult *results[qlen];
  while (pos) {
    results[--pos] = heap_poll(pq);
  }
  heap_free(pq);

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int len = 1;
  RedisModule_ReplyWithLongLong(ctx, total);
  for (pos = req->offset; pos < qlen && pos < req->offset + req->limit; pos++) {
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

/* DFT.ADD {index} ... */
int SingleShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, cmd.args[0] + 1);

  /* Rewrite the sharding key based on the partitioning key */
  SearchCluster_RewriteCommand(&__searchCluster, &cmd, 2);
  /* Rewrite the partitioning key as well */
  SearchCluster_RewriteCommandArg(&__searchCluster, &cmd, 2, 2);

  MR_MapSingle(MR_CreateCtx(ctx, NULL), singleReplyReducer, cmd);

  return REDISMODULE_OK;
}

int MastersFanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  printf("DFT CREATE!\n\n\n\n");
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, cmd.args[0] + 1);

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, allOKReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int FanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, cmd.args[0] + 1);

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), allOKReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int BroadcastCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc - 1, &argv[1]);
  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);
  MR_Fanout(mctx, chainReplyReducer, cmd);

  return REDISMODULE_OK;
}

int LocalSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // MR_UpdateTopology(ctx);
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

  // replace the LIMIT {offset} {limit} with LIMIT 0 {limit}, because we need all top N to merge
  int limitIndex = RMUtil_ArgExists("LIMIT", argv, argc, 3);
  if (limitIndex && req->limit > 0 && limitIndex < argc - 2) {
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0");
  }

  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "FT.SEARCH");
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_LocalCoordination | MRCluster_MastersOnly);

  MR_Map(mrctx, searchResultReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int SearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  // MR_UpdateTopology(ctx);

  // If this a one-node cluster, we revert to a simple, flat one level coordination
  // if (MR_NumHosts() < 2) {
  //   return LocalSearchCommandHandler(ctx, argv, argc);
  // }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_ReplaceArg(&cmd, 0, "DFT.LSEARCH");

  searchRequestCtx *req = parseRequest(argv, argc);
  if (!req) {
    return RedisModule_ReplyWithError(ctx, "Invalid search request");
  }
  // Internally we must have WITHSCORES set, even if the usr didn't set it
  if (!req->withScores) {
    MRCommand_AppendArgs(&cmd, 1, "WITHSCORES");
  }
  // MRCommand_Print(&cmd);

  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  MR_SetCoordinationStrategy(mrctx, MRCluster_RemoteCoordination);
  MR_Fanout(mrctx, searchResultReducer, cmd);

  return REDIS_OK;
}

int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RedisModule_AutoMemory(ctx);

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  RedisModule_ReplyWithSimpleString(ctx, "num_partitions");
  n++;
  RedisModule_ReplyWithLongLong(ctx, clusterConfig.numPartitions);
  n++;
  RedisModule_ReplyWithSimpleString(ctx, "cluster_type");
  n++;
  RedisModule_ReplyWithSimpleString(
      ctx, clusterConfig.type == ClusterType_RedisLabs ? "redislabs" : "redis_oss");
  n++;

  RedisModule_ReplyWithSimpleString(ctx, "slots");
  n++;
  MRClusterTopology *topo = MR_GetCurrentTopology();
  if (!topo) {
    RedisModule_ReplyWithNull(ctx);
    n++;

  } else {
    for (int i = 0; i < topo->numShards; i++) {
      MRClusterShard *sh = &topo->shards[i];
      RedisModule_ReplyWithArray(ctx, 2 + sh->numNodes);
      n++;
      RedisModule_ReplyWithLongLong(ctx, sh->startSlot);
      RedisModule_ReplyWithLongLong(ctx, sh->endSlot);
      for (int j = 0; j < sh->numNodes; j++) {
        MRClusterNode *node = &sh->nodes[j];
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithSimpleString(ctx, node->id);
        RedisModule_ReplyWithSimpleString(ctx, node->endpoint.host);
        RedisModule_ReplyWithLongLong(ctx, node->endpoint.port);
      }
    }
  }

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
}

// A special command for redis cluster OSS, that refreshes the cluster state
int RefreshClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RedisModule_AutoMemory(ctx);
  MRClusterTopology *topo = RedisCluster_GetTopology(ctx);
  MR_UpdateTopology(topo);
  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

int SetClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argc);
  // this means a parsing error, the parser already sent the explicit error to the client
  if (!topo) {
    return REDISMODULE_ERR;
  }
  // send the topology to the cluster
  if (MR_UpdateTopology(topo) != REDISMODULE_OK) {
    // failed update
    MRClusterTopology_Free(topo);
    return RedisModule_ReplyWithError(ctx, "Error updating the topology");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

/* Perform basic configurations and init all threads and global structures */
int initSearchCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  /* Init cluster configs */
  if (argc > 0) {
    if (ParseConfig(&clusterConfig, ctx, argv, argc) == REDISMODULE_ERR) {
      // printf("Could not parse module config\n");
      RedisModule_Log(ctx, "warning", "Could not parse module config");
      return REDISMODULE_ERR;
    }
  } else {
    RedisModule_Log(ctx, "warning", "No module config, reverting to default settings");
    clusterConfig = DEFAULT_CLUSTER_CONFIG;
  }

  RedisModule_Log(ctx, "notice", "Cluster configuration: %d partitions, type: %d",
                  clusterConfig.numPartitions, clusterConfig.type);

  ShardFunc sf;
  Partitioner pt;
  MRClusterTopology *initialTopology = NULL;
  switch (clusterConfig.type) {
    case ClusterType_RedisLabs:
      sf = CRC12ShardFunc;
      pt = NewSimplePartitioner(clusterConfig.numPartitions, crc12_slot_table, 4096);
      break;
    case ClusterType_RedisOSS:
    default:
      // init the redis topology pro
      if (InitRedisTopologyUpdater(clusterConfig.myEndpoint) == REDIS_ERR) {
        RedisModule_Log(ctx, "error", "Could not init redis cluster topology updater. Aborting");
        return REDISMODULE_ERR;
      }
      sf = CRC16ShardFunc;
      pt = NewSimplePartitioner(clusterConfig.numPartitions, crc16_slot_table, 16384);
  }

  MRCommand ping = MR_NewCommand(2, "FT.REPAIR", "rd{05S}");
  MRCommand_Print(&ping);
  if (clusterConfig.myEndpoint != NULL) {
    __periodicGC = NewPeriodicCommandRunner(&ping, clusterConfig.myEndpoint, 1000,
                                            periodicGCHandler, "rd{05S}");
  }

  MRCluster *cl = MR_NewCluster(initialTopology, sf, 2);
  MR_Init(cl);
  __searchCluster = NewSearchCluster(clusterConfig.numPartitions, pt);

  return REDISMODULE_OK;
}
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, "dft", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Init the configuration and global cluster structs
  if (initSearchCluster(ctx, argv, argc) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "error", "Could not init MR search cluster");
    return REDISMODULE_ERR;
  }

  /*********************************************************
  * Single-shard simple commands
  **********************************************************/
  if (RedisModule_CreateCommand(ctx, "dft.add", SingleShardCommandHandler, "readonly", 0, 0, -1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "dft.del", SingleShardCommandHandler, "readonly", 0, 0, -1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "dft.addhash", SingleShardCommandHandler, "readonly", 0, 0,
                                -1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /*********************************************************
   * Multi shard, fanout commands
   **********************************************************/
  if (RedisModule_CreateCommand(ctx, "dft.create", MastersFanoutCommandHandler, "readonly", 0, 0,
                                -1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "dft.drop", MastersFanoutCommandHandler, "readonly", 0, 0,
                                -1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "dft.broadcast", BroadcastCommand, "readonly", 0, 0, -1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /*********************************************************
  * Complex coordination search commands
  **********************************************************/
  if (RedisModule_CreateCommand(ctx, "dft.lsearch", LocalSearchCommandHandler, "readonly", 0, 0,
                                -1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "dft.search", SearchCommandHandler, "readonly", 0, 0, -1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  /*********************************************************
  * RS Cluster specific commands
  **********************************************************/
  if (RedisModule_CreateCommand(ctx, "dft.clusterset", SetClusterCommand, "readonly", 0, 0, -1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "dft.clusterrefresh", RefreshClusterCommand, "readonly", 0, 0,
                                -1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "dft.clusterinfo", ClusterInfoCommand, "readonly", 0, 0, -1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
