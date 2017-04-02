#ifndef __MR_COMMAND_H__
#define __MR_COMMAND_H__
#include <redismodule.h>

/* A redis command is represented with all its arguments and its flags as MRCommand */
typedef struct {
  /* The command args starting from the command itself */
  char **args;
  /* Number of arguments */
  int num;

  /* Internal id used to get the command configuration */
  int id;
} MRCommand;

/* Free the command and all its strings. Doesn't free the actual commmand struct, as it is usually
 * allocated on the stack */
void MRCommand_Free(MRCommand *cmd);

/* Createa a new command from an argv list of strings */
MRCommand MR_NewCommandArgv(int argc, char **argv);
/* Variadic creation of a command from a list of strings */
MRCommand MR_NewCommand(int argc, ...);
/* Create a command from a list of redis strings */
MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv);

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

void MRCommand_AppendArgs(MRCommand *cmd, int num, ...);

void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg);

int MRCommand_GetShardingKey(MRCommand *cmd);
typedef enum {
  MRCommand_SingleKey,
  MRCommand_MultiKey,
  MRCommand_Read,
  MRCommand_Write,
  MRCommand_Coordination,
} MRCommandFlags;

MRCommandFlags MRCommand_GetFlags(MRCommand *cmd);

/* Return 1 if the command should not be sharded */
int MRCommand_IsUnsharded(MRCommand *cmd);

void MRCommand_Print(MRCommand *cmd);
/* Create a copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(MRCommand *cmd);

#endif