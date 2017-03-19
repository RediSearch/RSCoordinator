#include "command.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

void MRCommand_Free(MRCommand *cmd) {
  for (int i = 0; i < cmd->num; i++) {
    free(cmd->args[i]);
  }
  free(cmd->command);
  free(cmd->args);
}

MRCommand MR_NewCommandArgv(int argc, char **argv) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char **)), .keyPos = -1};

  for (int i = 0; i < argc; i++) {
    if (i == 0) {
      cmd.command = strdup(argv[i]);
    } else {
      cmd.args[i - 1] = strdup(argv[i]);
    }
  }

  return cmd;
}

/* Create a deep copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(MRCommand *cmd) {
  MRCommand ret = *cmd;
  ret.command = strdup(cmd->command);
  ret.args = calloc(cmd->num, sizeof(char *));
  for (int i = 0; i < cmd->num; i++) {
    ret.args[i] = strdup(cmd->args[i]);
  }
  return ret;
}

MRCommand MR_NewCommand(const char *command, int argc, ...) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char **)), .keyPos = -1};
  cmd.command = strdup(command);
  va_list ap;
  va_start(ap, argc);
  for (int i = 0; i < argc; i++) {
    cmd.args[i] = strdup(va_arg(ap, const char *));
  }
  va_end(ap);

  return cmd;
}

MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char **)), .keyPos = -1};
  for (int i = 0; i < argc; i++) {
    if (i == 0) {
      cmd.command = strdup(RedisModule_StringPtrLen(argv[i], NULL));
    } else {
      cmd.args[i - 1] = strdup(RedisModule_StringPtrLen(argv[i], NULL));
    }
  }
  return cmd;
}

int MRCommand_GetShardingKey(MRCommand *cmd) {
  if (cmd->keyPos == -1) {
    return 0;
  }
  return cmd->keyPos;
}

void MRCommand_Print(MRCommand *cmd) {
  printf("%s ", cmd->command);
  for (int i = 0; i < cmd->num; i++) {
    printf("%s ", cmd->args[i]);
  }
  printf("\n");
}
