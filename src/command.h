#ifndef __MR_COMMAND_H__
#define __MR_COMMAND_H__
#include "redismodule.h"

typedef struct {
    char **args;
    int num;
} MRCommand;

void MRCommand_Free(MRCommand *cmd);

MRCommand MR_NewCommandArgv(int argc, char **argv);
MRCommand MR_NewCommand(int argc, ...);
MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv);

#endif