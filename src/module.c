#include "redismodule.h"
#include "rmr.h"
#include "hiredis/async.h"
#include <stdlib.h>



typedef struct MREndpoint {
  char *host;
  int port;
  redisAsyncContext *c;
} MREndpoint;

static struct MRCluster *cl = NULL;


int myReducer(struct MRCtx *mc, int count, redisReply **replies) {

    RedisModuleCtx *ctx = MRCtx_GetPrivdata(mc);

    RedisModule_ReplyWithLongLong(ctx, count);
    return REDISMODULE_OK;
}

int TestCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    struct MRCtx *mc = MR_CreateCtx(cl, ctx);

    const char **args = calloc(1, sizeof(char *));
    args[0] = "PING";
    MR_Map(mc, myReducer, 1,  args );

    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    
    //if (cl == NULL) {
        int N = 4;

        MREndpoint *nodes = calloc(N, sizeof(MREndpoint));
        for (int i = 0; i < N; i++) {
            nodes[i] = (MREndpoint){"localhost", 6378, NULL};
        }
        cl = MR_NewCluster(N, nodes);

      MR_Init(cl);

//    }


  
  if (RedisModule_Init(ctx, "rmr", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // register index type

  if (RedisModule_CreateCommand(ctx, "rmr.test", TestCmd, "readonly", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

 
  return REDISMODULE_OK;
}