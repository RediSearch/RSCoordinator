#include "command.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

void MRCommand_Free(MRCommand *cmd) {
  for (int i = 0; i < cmd->num; i++) {
    free(cmd->args[i]);
  }

  free(cmd->args);
}

MRCommand MR_NewCommandArgv(int argc, char **argv) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char **)), .keyPos = -1};

  for (int i = 0; i < argc; i++) {

    cmd.args[i] = strdup(argv[i]);
  }

  return cmd;
}

/* Create a deep copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(MRCommand *cmd) {
  MRCommand ret = *cmd;

  ret.args = calloc(cmd->num, sizeof(char *));
  for (int i = 0; i < cmd->num; i++) {
    ret.args[i] = strdup(cmd->args[i]);
  }
  return ret;
}

MRCommand MR_NewCommand(int argc, ...) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char *)), .keyPos = -1};

  va_list ap;
  va_start(ap, argc);
  for (int i = 0; i < argc; i++) {
    cmd.args[i] = strdup(va_arg(ap, const char *));
  }
  va_end(ap);

  return cmd;
}

MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char *)), .keyPos = -1};
  for (int i = 0; i < argc; i++) {
    cmd.args[i] = strdup(RedisModule_StringPtrLen(argv[i], NULL));
  }
  return cmd;
}

void MRCommand_AppendArgs(MRCommand *cmd, int num, ...) {
  if (num <= 0) return;
  int oldNum = cmd->num;
  cmd->num += num;

  cmd->args = realloc(cmd->args, cmd->num * sizeof(*cmd->args));
  va_list(ap);
  va_start(ap, num);
  for (int i = oldNum; i < cmd->num; i++) {
    cmd->args[i] = strdup(va_arg(ap, const char *));
  }
  va_end(ap);
}

void MRCommand_SetKeyPos(MRCommand *cmd, int keyPos) {
  cmd->keyPos = keyPos;
}

void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg) {
  if (index < 0 || index >= cmd->num) {
    return;
  }
  char *tmp = cmd->args[index];
  cmd->args[index] = strdup(newArg);
  free(tmp);
}
int MRCommand_GetShardingKey(MRCommand *cmd) {
  if (cmd->keyPos == -1) {
    return cmd->num > 1 ? 1 : 0;
  }
  return cmd->keyPos;
}

void MRCommand_Print(MRCommand *cmd) {

  for (int i = 0; i < cmd->num; i++) {
    printf("%s ", cmd->args[i]);
  }
  printf("\n");
}
