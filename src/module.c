#include <stdlib.h>
#include <string.h>

#include "redismodule.h"
#include "rmr.h"
#include "hiredis/async.h"
#include "reply.h"



int chainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetPrivdata(mc);

  RedisModule_ReplyWithArray(ctx, count);
  for (int i = 0; i < count; i++) {
    __mr_replyWithMRReply(ctx, replies[i]);
  }
  return REDISMODULE_OK;
}

int sumReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetPrivdata(mc);
  long long sum = 0;
  for (int i = 0; i < count; i++) {
    long long n = 0;
    if (MRReply_ToInteger(replies[i], &n)) {
      sum += n;
    }
  }

  return RedisModule_ReplyWithLongLong(ctx, sum);
  
}

int SumAggCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char **args = calloc(2, sizeof(char *));
  args[0] = "GET";
  args[1] = RedisModule_StringPtrLen(argv[1], NULL);
  
  struct MRCtx *mc = MR_CreateCtx(ctx);
  MR_Map(mc, sumReducer,2 , args);

  return REDISMODULE_OK;
}

int TestCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *args[argc-1];
  for (int i = 0; i < argc - 1; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i + 1], NULL);
  }

  struct MRCtx *mc = MR_CreateCtx(ctx);
  MR_Map(mc, chainReplyReducer, argc - 1, args);
  
  return REDISMODULE_OK;
}


int RedisModule_OnLoad(RedisModuleCtx *ctx) {

  // if (cl == NULL) {
  int N = 4;
MR_Init(MR_NewDummyNodeProvider(N, 6375));

  //    }

  if (RedisModule_Init(ctx, "rmr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // register index type

  if (RedisModule_CreateCommand(ctx, "rmr.test", TestCmd, "readonly", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rmr.sum", SumAggCmd, "readonly", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}