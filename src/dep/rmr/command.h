#ifndef __MR_COMMAND_H__
#define __MR_COMMAND_H__
#include <redismodule.h>

typedef struct {
  char **args;
  int num;
  int keyPos;
} MRCommand;

/* A generator producing a list of commands on successive calls to Next(); */
typedef struct {
  /* Private context of what's actually going on */
  void *ctx;

  /* The number of commands in this generator. We must know it in advance */
  size_t (*Len)(void *ctx);

  /* Next callback - should yield 0 if we are at the end, 1 if not, and put the next valud in cmd */
  int (*Next)(void *ctx, MRCommand *cmd);

  /* Free callback - used to free the private context */
  void (*Free)(void *ctx);
} MRCommandGenerator;

/* Free the command and all its strings. Doesn't free the actual commmand struct, as it is usually
 * allocated on the stack */
void MRCommand_Free(MRCommand *cmd);

MRCommand MR_NewCommandArgv(int argc, char **argv);
MRCommand MR_NewCommand(int argc, ...);
MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv);

void MRCommand_AppendArgs(MRCommand *cmd, int num, ...);

void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg);

void MRCommand_SetKeyPos(MRCommand *cmd, int keyPos);
int MRCommand_GetShardingKey(MRCommand *cmd);

void MRCommand_Print(MRCommand *cmd);
/* Create a copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(MRCommand *cmd);
#endif