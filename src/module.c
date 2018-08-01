#include <stdlib.h>
#include <string.h>

#include "redismodule.h"
#include "dep/rmr/rmr.h"
#include "dep/rmr/hiredis/async.h"
#include "dep/rmr/reply.h"
#include <rmutil/util.h>
#include <rmutil/strings.h>
#include "crc16_tags.h"
#include "crc12_tags.h"
#include "dep/rmr/redis_cluster.h"
#include "dep/rmr/redise.h"
#include "fnv32.h"
#include "search_cluster.h"
#include "config.h"
#include "dep/RediSearch/src/module.h"
#include <math.h>
#include "info_command.h"
#include "version.h"
#include "cursor.h"
#include "build-info/info.h"
#include <sys/param.h>
#include <pthread.h>
#include <aggregate/aggregate.h>
#include <value.h>
#include <stdbool.h>
#include "cluster_spell_check.h"
#include "dist_search.h"

// forward declaration
int allOKReducer(struct MRCtx *mc, int count, MRReply **replies);
RSValue *MRReply_ToValue(MRReply *r, RSValueType convertType);

/* A reducer that just chains the replies from a map request */
int chainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

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
int uniqueStringsReducer(struct MRCtx *mc, int count, MRReply **replies) {
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
int mergeArraysReducer(struct MRCtx *mc, int count, MRReply **replies) {

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

int synonymAddFailedReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    return RedisModule_ReplyWithNull(ctx);
  }

  MR_ReplyWithMRReply(ctx, replies[0]);

  return REDISMODULE_OK;
}

int synonymAllOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
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

  assert(MRCtx_GetCmdsSize(mc) >= 1);
  assert(MRCtx_GetCmds(mc)[0].num > 3);
  size_t groupLen;
  const char *groupStr = MRCommand_ArgStringPtrLen(&MRCtx_GetCmds(mc)[0], 2, &groupLen);
  RedisModuleString *synonymGroupIdStr = RedisModule_CreateString(ctx, groupStr, groupLen);
  long long synonymGroupId = 0;
  int rv = RedisModule_StringToLongLong(synonymGroupIdStr, &synonymGroupId);
  assert(rv == REDIS_OK);

  RedisModule_ReplyWithLongLong(ctx, synonymGroupId);

  RedisModule_FreeString(ctx, synonymGroupIdStr);
  return REDISMODULE_OK;
}

int synonymUpdateFanOutReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count != 1) {
    RedisModuleBlockedClient *bc = (RedisModuleBlockedClient *)ctx;
    RedisModule_UnblockClient(bc, mc);
    return REDISMODULE_OK;
  }
  if (MRReply_Type(replies[0]) != MR_REPLY_INTEGER) {
    RedisModuleBlockedClient *bc = (RedisModuleBlockedClient *)ctx;
    RedisModule_UnblockClient(bc, mc);
    return REDISMODULE_OK;
  }
  assert(MRCtx_GetCmdsSize(mc) == 1);
  MRCommand updateCommand = {NULL};
  const MRCommand *srcCmd = &MRCtx_GetCmds(mc)[0];
  for (size_t ii = 0; ii < 2; ++ii) {
    MRCommand_AppendFrom(&updateCommand, srcCmd, ii);
  }

  // MRCommand updateCommand = MR_NewCommandFromStrings(2, MRCtx_GetCmds(mc)[0].args);
  double d = 0;
  MRReply_ToDouble(replies[0], &d);
  char buf[128] = {0};
  size_t nbuf = sprintf(buf, "%lu", (unsigned long)d);
  MRCommand_Append(&updateCommand, buf, nbuf);

  for (size_t ii = 2; ii < srcCmd->num; ++ii) {
    MRCommand_AppendFrom(&updateCommand, srcCmd, ii);
  }

  const char *cmdName = "_FT.SYNFORCEUPDATE";
  MRCommand_ReplaceArg(&updateCommand, 0, cmdName, strlen(cmdName));

  size_t idLen = 0;
  const char *idStr = MRCommand_ArgStringPtrLen(&updateCommand, 1, &idLen);
  MRKey key = {0};
  MRKey_Parse(&key, idStr, idLen);

  // reseting the tag
  MRCommand_ReplaceArg(&updateCommand, 1, key.base, key.baseLen);

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &updateCommand);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly);
  MR_Map(mrctx, synonymAllOKReducer, cg, false);
  cg.Free(cg.ctx);

  // we need to call request complete here manualy since we did not unblocked the client
  MR_requestCompleted();
  return REDISMODULE_OK;
}

int singleReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    return RedisModule_ReplyWithNull(ctx);
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
  bool isIntegerReply = false;
  long long integerReply = 0;
  for (int i = 0; i < count; i++) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(ctx, replies[i]);
      return REDISMODULE_OK;
    }
    if (MRReply_Type(replies[i]) == MR_REPLY_INTEGER) {
      long long currIntegerReply = MRReply_Integer(replies[i]);
      if (!isIntegerReply) {
        integerReply = currIntegerReply;
        isIntegerReply = true;
      } else if (currIntegerReply != integerReply) {
        RedisModule_ReplyWithSimpleString(ctx, "not all results are the same");
        return REDISMODULE_OK;
      }
    }
  }

  if (isIntegerReply) {
    RedisModule_ReplyWithLongLong(ctx, integerReply);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  return REDISMODULE_OK;
}

int FirstPartitionCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 MRReduceFunc reducer, struct MRCtx *mrCtx) {

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  /* Rewrite the sharding key based on the partitioning key */
  SearchCluster_RewriteCommandToFirstPartition(GetSearchCluster(), &cmd);

  MR_MapSingle(mrCtx, reducer, cmd);

  return REDISMODULE_OK;
}

int FirstShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }

  RedisModule_AutoMemory(ctx);

  struct MRCtx *mrCtx = MR_CreateCtx(ctx, NULL);

  return FirstPartitionCommandHandler(ctx, argv, argc, singleReplyReducer, mrCtx);
}

int SynAddCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }

  RedisModule_AutoMemory(ctx);

  struct MRCtx *mrCtx = MR_CreateCtx(ctx, NULL);

  // reducer is set here so the client will not be unblocked.
  // we need to send SYNFORCEUPDATE commands to the other
  // shards before unblocking the client.
  MRCtx_SetReduceFunction(mrCtx, synonymUpdateFanOutReducer);

  return FirstPartitionCommandHandler(ctx, argv, argc, synonymAddFailedReducer, mrCtx);
}

/* ft.ADD {index} ... */
int SingleShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  int partPos = MRCommand_GetPartitioningKey(&cmd);

  /* Rewrite the sharding key based on the partitioning key */
  if (partPos > 0) {
    SearchCluster_RewriteCommand(GetSearchCluster(), &cmd, partPos);
  }

  /* Rewrite the partitioning key as well */

  if (MRCommand_GetFlags(&cmd) & MRCommand_MultiKey) {
    if (partPos > 0) {
      SearchCluster_RewriteCommandArg(GetSearchCluster(), &cmd, partPos, partPos);
    }
  }
  // MRCommand_Print(&cmd);
  MR_MapSingle(MR_CreateCtx(ctx, NULL), singleReplyReducer, cmd);

  return REDISMODULE_OK;
}

/* FT.MGET {idx} {key} ... */
int MGetCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  for (int i = 2; i < argc; i++) {
    SearchCluster_RewriteCommandArg(GetSearchCluster(), &cmd, i, i);
  }

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, mergeArraysReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int SpellCheckCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, spellCheckReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int MastersFanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, allOKReducer, cg, true);
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

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), allOKReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

void AggregateCommand_ExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                        struct ConcurrentCmdCtx *ccx);

static int DIST_AGG_THREADPOOL = -1;

static int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  return ConcurrentSearch_HandleRedisCommandEx(DIST_AGG_THREADPOOL, CMDCTX_NO_GIL,
                                               AggregateCommand_ExecDistAggregate, ctx, argv, argc);
}

static int CursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 1) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  return ConcurrentSearch_HandleRedisCommandEx(DIST_AGG_THREADPOOL, CMDCTX_NO_GIL,
                                               AggregateCommand_ExecCursor, ctx, argv, argc);
}

int TagValsCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), uniqueStringsReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int BroadcastCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc - 1, &argv[1]);
  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);

  if (cmd.num > 1 && MRCommand_GetShardingKey(&cmd) >= 0) {
    MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
    MR_Map(mctx, chainReplyReducer, cg, true);
    cg.Free(cg.ctx);
  } else {
    MR_Fanout(mctx, chainReplyReducer, cmd);
  }
  return REDISMODULE_OK;
}

int InfoCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    // FT.INFO {index}
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModule_AutoMemory(ctx);
  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);
  MR_Map(mctx, InfoReplyReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RedisModule_AutoMemory(ctx);

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  RedisModule_ReplyWithSimpleString(ctx, "num_partitions");
  n++;
  RedisModule_ReplyWithLongLong(ctx, GetSearchCluster()->size);
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

  SearchCluster_EnsureSize(ctx, GetSearchCluster(), topo);

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

  SearchCluster_EnsureSize(ctx, GetSearchCluster(), topo);
  // If the cluster hash func or cluster slots has changed, set the new value
  switch (topo->hashFunc) {
    case MRHashFunc_CRC12:
      PartitionCtx_SetSlotTable(&GetSearchCluster()->part, crc12_slot_table,
                                MIN(4096, topo->numSlots));
      break;
    case MRHashFunc_CRC16:
      PartitionCtx_SetSlotTable(&GetSearchCluster()->part, crc16_slot_table,
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
  InitGlobalSearchCluster(clusterConfig.numPartitions, slotTable, tableSize);

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
  if (RSBuildType_g == RSBuildType_Enterprise && clusterConfig.type != ClusterType_RedisLabs) {
    /* If we are running inside OSS cluster and not built for oss, we return the dummy handler */
    return DisabledCommandHandler;
  }

  /* Valid - we return the original function */
  return f;
}

/**
 * Because our indexes are in the form of IDX{something}, a single real index might
 * appear as multiple indexes, using more memory and essentially disabling rate
 * limiting.
 *
 * This works as a hook after every new index is created, to strip the '{' from the
 * cursor name, and use the real name as the entry.
 */
static void addIndexCursor(const IndexSpec *sp) {
  char *s = strdup(sp->name);
  char *end = strchr(s, '{');
  if (end) {
    *end = '\0';
    CursorList_AddSpec(&RSCursors, s, RSCURSORS_DEFAULT_CAPACITY);
  }
  free(s);
}

#define RM_TRY(expr)                                                  \
  if (expr == REDISMODULE_ERR) {                                      \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(expr)); \
    return REDISMODULE_ERR;                                           \
  }

int __attribute__((visibility("default")))
RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  IndexSpec_OnCreate = addIndexCursor;
  /**

  FT.AGGREGATE gh * LOAD 1 @type GROUPBY 1 @type REDUCE COUNT 0 AS num REDUCE SUM 1 @date SORTBY 2
  @num DESC MAX 10

   */

  printf("RSValue size: %lu\n", sizeof(RSValue));

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

  // Init the aggregation thread pool
  DIST_AGG_THREADPOOL = ConcurrentSearch_CreatePool(RSGlobalConfig.searchPoolSize);

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
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.AGGREGATE", SafeCmd(DistAggregateCommand), "readonly",
                                   0, 1, -2));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.CREATE", SafeCmd(MastersFanoutCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALTER", SafeCmd(MastersFanoutCommandHandler),
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

  /**
   * Self-commands. These are executed directly on the server
   */
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 0, 0, -1));

  /**
   * Synonym Support
   */
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNADD", SafeCmd(SynAddCommandHandler), "readonly", 0,
                                   0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNDUMP", SafeCmd(FirstShardCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNUPDATE", SafeCmd(MastersFanoutCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNFORCEUPDATE", SafeCmd(MastersFanoutCommandHandler),
                                   "readonly", 0, 0, -1));

  /**
   * Dictionary commands
   */
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTADD", SafeCmd(MastersFanoutCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTDEL", SafeCmd(MastersFanoutCommandHandler),
                                   "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTDUMP", SafeCmd(FirstShardCommandHandler),
                                   "readonly", 0, 0, -1));

  /**
   * spell check
   */
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SPELLCHECK", SafeCmd(SpellCheckCommandHandler),
                                   "readonly", 0, 0, -1));

  return REDISMODULE_OK;
}
