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
#include "config.h"
#include "dep/RediSearch/src/module.h"
#include "info_command.h"
#include "reducers.h"
#include "search_reducer.h"
#include "version.h"
#include <sys/param.h>

SearchCluster __searchCluster;
#define CLUSTERDOWN_ERR "Uninitialized cluster state, could not perform command"




/* ft.ADD {index} ... */
int SingleShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  int partPos = MRCommand_GetPartitioningKey(&cmd);

  /* Rewrite the sharding key based on the partitioning key */
  if (partPos > 0) {
    SearchCluster_RewriteCommand(&__searchCluster, &cmd, partPos);
  }

  /* Rewrite the partitioning key as well */

  if (MRCommand_GetFlags(&cmd) & MRCommand_MultiKey) {
    if (partPos > 0) {
      SearchCluster_RewriteCommandArg(&__searchCluster, &cmd, partPos, partPos);
    }
  }
  // MRCommand_Print(&cmd);
  MR_MapSingle(MR_CreateCtx(ctx, NULL), SingleReplyReducer, cmd);

  return REDISMODULE_OK;
}

/* FT.MGET {idx} {key} ... */
int MGetCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  for (int i = 2; i < argc; i++) {
    SearchCluster_RewriteCommandArg(&__searchCluster, &cmd, i, i);
  }

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, MergeArraysReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int MastersFanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, AllOKReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int FanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), AllOKReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int TagValsCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), UniqueStringsReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int BroadcastCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc - 1, &argv[1]);
  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);

  if (cmd.num > 1 && MRCommand_GetShardingKey(&cmd) >= 0) {
    MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
    MR_Map(mctx, ChainReplyReducer, cg);
    cg.Free(cg.ctx);
  } else {
    MR_Fanout(mctx, ChainReplyReducer, cmd);
  }
  return REDISMODULE_OK;
}

int InfoCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    // FT.INFO {index}
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);
  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);
  MR_Map(mctx, InfoReplyReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int LocalSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // MR_UpdateTopology(ctx);
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
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
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0");
  }

  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "_FT.SEARCH");
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_LocalCoordination | MRCluster_MastersOnly);

  MR_Map(mrctx, SearchResultReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int FlatSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // MR_UpdateTopology(ctx);
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
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
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0");
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", req->limit + req->offset);
    MRCommand_ReplaceArg(&cmd, limitIndex + 2, buf);
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
        SearchCluster_RewriteCommandArg(&__searchCluster, &cmd, x, x);
      }
    }
  }
  // MRCommand_Print(&cmd);

  /* Replace our own FT command with _FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "_FT.SEARCH");
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&__searchCluster, &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_FlatCoordination);

  MR_Map(mrctx, SearchResultReducer, cg);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int SearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(&__searchCluster)) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);
  // MR_UpdateTopology(ctx);

  // If this a one-node cluster, we revert to a simple, flat one level coordination
  // if (MR_NumHosts() < 2) {
  //   return LocalSearchCommandHandler(ctx, argv, argc);
  // }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_ReplaceArg(&cmd, 0, "FT.LSEARCH");

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

int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RedisModule_AutoMemory(ctx);

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  RedisModule_ReplyWithSimpleString(ctx, "num_partitions");
  n++;
  RedisModule_ReplyWithLongLong(ctx, __searchCluster.size);
  n++;
  RedisModule_ReplyWithSimpleString(ctx, "cluster_type");
  n++;
  RedisModule_ReplyWithSimpleString(
      ctx, clusterConfig.type == ClusterType_RedisLabs ? "redislabs" : "redis_oss");
  n++;

  // Report hash func
  MRClusterTopology *topo = MR_GetCurrentTopology();
  RedisModule_ReplyWithSimpleString(ctx, "hash_func");
  n++;
  if (topo) {
    RedisModule_ReplyWithSimpleString(
        ctx, topo->hashFunc == MRHashFunc_CRC12
                 ? MRHASHFUNC_CRC12_STR
                 : (topo->hashFunc == MRHashFunc_CRC16 ? MRHASHFUNC_CRC16_STR : "n/a"));
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "n/a");
  }
  n++;

  // Report topology
  RedisModule_ReplyWithSimpleString(ctx, "num_slots");
  n++;
  RedisModule_ReplyWithLongLong(ctx, topo ? (long long)topo->numSlots : 0);
  n++;

  RedisModule_ReplyWithSimpleString(ctx, "slots");
  n++;

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
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithSimpleString(ctx, node->id);
        RedisModule_ReplyWithSimpleString(ctx, node->endpoint.host);
        RedisModule_ReplyWithLongLong(ctx, node->endpoint.port);
        RedisModule_ReplyWithString(
            ctx, RedisModule_CreateStringPrintf(ctx, "%s%s",
                                                node->flags & MRNode_Master ? "master " : "slave ",
                                                node->flags & MRNode_Self ? "self" : ""));
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

  SearchCluster_EnsureSize(ctx, &__searchCluster, topo);

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

  SearchCluster_EnsureSize(ctx, &__searchCluster, topo);
  // If the cluster hash func or cluster slots has changed, set the new value
  switch (topo->hashFunc) {
    case MRHashFunc_CRC12:
      PartitionCtx_SetSlotTable(&__searchCluster.part, crc12_slot_table, MIN(4096, topo->numSlots));
      break;
    case MRHashFunc_CRC16:
      PartitionCtx_SetSlotTable(&__searchCluster.part, crc16_slot_table,
                                MIN(16384, topo->numSlots));
      break;
    case MRHashFunc_None:
    default:
      // do nothing
      break;
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

  RedisModule_Log(ctx, "notice",
                  "Cluster configuration: %d partitions, type: %d, coordinator timeout: %dms",
                  clusterConfig.numPartitions, clusterConfig.type, clusterConfig.timeoutMS);

  /* Configure cluster injections */
  ShardFunc sf;

  MRClusterTopology *initialTopology = NULL;

  const char **slotTable = NULL;
  size_t tableSize = 0;

  switch (clusterConfig.type) {
    case ClusterType_RedisLabs:
      sf = CRC12ShardFunc;
      slotTable = crc12_slot_table;
      tableSize = 4096;

      break;
    case ClusterType_RedisOSS:
    default:
      // init the redis topology updater loop
      if (InitRedisTopologyUpdater() == REDIS_ERR) {
        RedisModule_Log(ctx, "warning", "Could not init redis cluster topology updater. Aborting");
        return REDISMODULE_ERR;
      }
      sf = CRC16ShardFunc;
      slotTable = crc16_slot_table;
      tableSize = 16384;
  }

  MRCluster *cl = MR_NewCluster(initialTopology, sf, 2);
  MR_Init(cl, clusterConfig.timeoutMS);
  __searchCluster = NewSearchCluster(clusterConfig.numPartitions, slotTable, tableSize);

  return REDISMODULE_OK;
}

/** A dummy command handler, for commands that are disabled when running the module in OSS
 * clusters
 * when it is not an internal OSS build. */
int DisabledCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, "Module Disabled in Open Source Redis");
}

/** A wrapper function that safely checks whether we are running in OSS cluster when registering
 * commands.
 * If we are, and the module was not compiled for oss clusters, this wrapper will return a pointer
 * to a dummy function disabling the actual handler.
 *
 * If we are running in RLEC or in a special OSS build - we simply return the original command.
 *
 * All coordinator handlers must be wrapped in this decorator.
 */
static RedisModuleCmdFunc SafeCmd(RedisModuleCmdFunc f) {

#ifndef REDISEARCH_OSS_BUILD
  /* If we are running inside OSS cluster and not built for oss, we return the dummy handler */
  if (clusterConfig.type == ClusterType_RedisOSS) {
    return DisabledCommandHandler;
  }
#endif
  /* Valid - we return the original function */
  return f;
}

#define RM_TRY(expr)                                                  \
  if (expr == REDISMODULE_ERR) {                                      \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(expr)); \
    return REDISMODULE_ERR;                                           \
  }

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, "ft", RSCOORDINATOR_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Init RediSearch internal search
  if (RediSearch_InitModuleInternal(ctx, argv, argc) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init search library...");
    return REDISMODULE_ERR;
  }

  // Init the configuration and global cluster structs
  if (initSearchCluster(ctx, argv, argc) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init MR search cluster");
    return REDISMODULE_ERR;
  }

  /*********************************************************
   * Single-shard simple commands
   **********************************************************/
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.ADD", SafeCmd(SingleShardCommandHandler), "readonly", 0,
                                   0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.DEL", SafeCmd(SingleShardCommandHandler), "readonly", 0,
                                   0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.GET", SafeCmd(SingleShardCommandHandler), "readonly", 0,
                                   0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.TAGVALS", SafeCmd(TagValsCommandHandler), "readonly", 0,
                                   0, -1));
  RM_TRY(
      RedisModule_CreateCommand(ctx, "FT.MGET", SafeCmd(MGetCommandHandler), "readonly", 0, 0, -1));

  RM_TRY(RedisModule_CreateCommand(ctx, "FT.ADDHASH", SafeCmd(SingleShardCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.EXPLAIN", SafeCmd(SingleShardCommandHandler),
                                   "readonly", 0, 0, -1));

  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGADD", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGGET", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGDEL", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGLEN", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));

  /*********************************************************
   * Multi shard, fanout commands
   **********************************************************/
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.CREATE", SafeCmd(MastersFanoutCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.DROP", SafeCmd(MastersFanoutCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.BROADCAST", SafeCmd(BroadcastCommand), "readonly", 0, 0,
                                   -1));
  RM_TRY(
      RedisModule_CreateCommand(ctx, "FT.INFO", SafeCmd(InfoCommandHandler), "readonly", 0, 0, -1));

  /*********************************************************
   * Complex coordination search commands
   **********************************************************/
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.LSEARCH", SafeCmd(LocalSearchCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.FSEARCH", SafeCmd(FlatSearchCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SEARCH", SafeCmd(FlatSearchCommandHandler), "readonly",
                                   0, 0, -1));

  /*********************************************************
   * RS Cluster specific commands
   **********************************************************/
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.CLUSTERSET", SafeCmd(SetClusterCommand), "readonly", 0,
                                   0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.CLUSTERREFRESH", SafeCmd(RefreshClusterCommand),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.CLUSTERINFO", SafeCmd(ClusterInfoCommand), "readonly",
                                   0, 0, -1));

  return REDISMODULE_OK;
}
