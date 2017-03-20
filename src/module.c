#include <stdlib.h>
#include <string.h>

#include "redismodule.h"
#include "rmr.h"
#include "hiredis/async.h"
#include "reply.h"
#include "fnv.h"
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

  if (RedisModule_CreateCommand(ctx, "dft.search", FanoutCommandHandler, "readonly", 1, 1, 1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}