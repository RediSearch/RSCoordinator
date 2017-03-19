#ifndef __MR_COMMAND_H__
#define __MR_COMMAND_H__
#include "redismodule.h"

typedef struct {
  char *command;
  char **args;
  int num;
  int keyPos;
} MRCommand;

void MRCommand_Free(MRCommand *cmd);

MRCommand MR_NewCommandArgv(int argc, char **argv);
MRCommand MR_NewCommand(const char *command, int argc, ...);
MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv);
void MRCommand_SetKeyPos(MRCommand *cmd, int keyPos);
int MRCommand_GetShardingKey(MRCommand *cmd);

void MRCommand_Print(MRCommand *cmd);
/* Create a copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(MRCommand *cmd);
#endif