#include "command.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/*
 * Forward declaration
 */
extern MRCommandGenerator spellCheckCommandGenerator;

struct mrCommandConf {
  const char *command;
  MRCommandFlags flags;
  int keyPos;
  int partitionKeyPos;
  MRCommandGenerator *commandGenerator;
};

struct mrCommandConf __commandConfig[] = {

    // document commands
    {"_FT.SEARCH", MRCommand_Read | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.DEL", MRCommand_Write | MRCommand_MultiKey, 1, 2, NULL},
    {"_FT.GET", MRCommand_Read | MRCommand_MultiKey, 1, 2, NULL},
    {"_FT.MGET", MRCommand_Read | MRCommand_MultiKey, 1, 2, NULL},

    {"_FT.ADD", MRCommand_Write | MRCommand_MultiKey, 1, 2, NULL},
    {"_FT.ADDHASH", MRCommand_Write | MRCommand_MultiKey, 1, 2, NULL},

    // index commands
    {"_FT.CREATE", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.ALTER", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.DROP", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.OPTIMIZE", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.INFO", MRCommand_Read | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.EXPLAIN", MRCommand_Read | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.TAGVALS", MRCommand_Read | MRCommand_SingleKey, 1, 1, NULL},

    // Suggest commands
    {"_FT.SUGADD", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.SUGGET", MRCommand_Read | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.SUGLEN", MRCommand_Read | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.SUGDEL", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.CURSOR", MRCommand_Read | MRCommand_SingleKey, 2, 2, NULL},

    // Synonyms commands
    {"_FT.SYNADD", MRCommand_Write | MRCommand_NoKey, 1, -1, NULL},
    {"_FT.SYNDUMP", MRCommand_Write | MRCommand_NoKey, 1, -1, NULL},
    {"_FT.SYNUPDATE", MRCommand_Write | MRCommand_NoKey, 1, -1, NULL},
    {"_FT.SYNFORCEUPDATE", MRCommand_Write | MRCommand_NoKey, 1, -1, NULL},

    // Coordination commands - they are all read commands since they can be triggered from slaves
    {"FT.ADD", MRCommand_Read | MRCommand_Coordination, -1, 2, NULL},
    {"FT.SEARCH", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.AGGREGATE", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},

    {"FT.EXPLAIN", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},

    {"FT.FSEARCH", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.CREATE", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.CLUSTERINFO", MRCommand_Read | MRCommand_Coordination, -1, -1, NULL},
    {"FT.INFO", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.ADDHASH", MRCommand_Read | MRCommand_Coordination, -1, 2, NULL},
    {"FT.DEL", MRCommand_Read | MRCommand_Coordination, -1, 2, NULL},
    {"FT.DROP", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.CREATE", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.GET", MRCommand_Read | MRCommand_Coordination, -1, 2, NULL},
    {"FT.MGET", MRCommand_Read | MRCommand_Coordination, -1, 2, NULL},

    // Auto complete coordination commands
    {"FT.SUGADD", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.SUGGET", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.SUGDEL", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},
    {"FT.SUGLEN", MRCommand_Read | MRCommand_Coordination, -1, 1, NULL},

    {"KEYS", MRCommand_Read | MRCommand_NoKey, -1, -1, NULL},
    {"INFO", MRCommand_Read | MRCommand_NoKey, -1, -1, NULL},
    {"SCAN", MRCommand_Read | MRCommand_NoKey, -1, -1, NULL},

    // dictionary commands
    {"_FT.DICTADD", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.DICTDEL", MRCommand_Write | MRCommand_SingleKey, 1, 1, NULL},
    {"_FT.DICTDUMP", MRCommand_Write | MRCommand_NoKey, 1, -1, NULL},

    // spell check
    {"_FT.SPELLCHECK", MRCommand_Write | MRCommand_NoKey, 1, -1, &spellCheckCommandGenerator},

    // sentinel
    {NULL},
};

int _getCommandConfId(MRCommand *cmd) {
  cmd->id = -1;
  if (cmd->num == 0) {
    return 0;
  }

  for (int i = 0; __commandConfig[i].command != NULL; i++) {
    if (!strcasecmp(cmd->args[0], __commandConfig[i].command)) {
      // printf("conf id for cmd %s: %d\n", cmd->args[0], i);
      cmd->id = i;
      return 1;
    }
  }
  return 0;
}

void MRCommand_Free(MRCommand *cmd) {
  for (int i = 0; i < cmd->num; i++) {
    free(cmd->args[i]);
  }

  free(cmd->args);
}

MRCommand MR_NewCommandArgv(int argc, char **argv) {
  MRCommand cmd = (MRCommand){.num = argc, .args = calloc(argc, sizeof(char *))};

  for (int i = 0; i < argc; i++) {

    cmd.args[i] = strdup(argv[i]);
  }
  _getCommandConfId(&cmd);
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
  MRCommand cmd = (MRCommand){
      .num = argc,
      .args = calloc(argc, sizeof(char *)),
  };

  va_list ap;
  va_start(ap, argc);
  for (int i = 0; i < argc; i++) {
    cmd.args[i] = strdup(va_arg(ap, const char *));
  }
  va_end(ap);
  _getCommandConfId(&cmd);
  return cmd;
}

MRCommand MR_NewCommandFromStrings(int argc, char** argv) {
  MRCommand cmd = (MRCommand){
      .num = argc,
      .args = calloc(argc, sizeof(char *)),
  };
  for (int i = 0; i < argc; i++) {
    cmd.args[i] = strdup(argv[i]);
  }
  _getCommandConfId(&cmd);
  return cmd;
}

MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv) {
  MRCommand cmd = (MRCommand){
      .num = argc,
      .args = calloc(argc, sizeof(char *)),
  };
  for (int i = 0; i < argc; i++) {
    cmd.args[i] = strdup(RedisModule_StringPtrLen(argv[i], NULL));
  }
  _getCommandConfId(&cmd);
  return cmd;
}

void MRCommand_AppendStringsArgs(MRCommand *cmd, int num, char** args) {
  if (num <= 0) return;
  int oldNum = cmd->num;
  cmd->num += num;

  cmd->args = realloc(cmd->args, cmd->num * sizeof(*cmd->args));
  for (int i = oldNum; i < cmd->num; i++) {
    cmd->args[i] = strdup(args[i - oldNum]);
  }
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

/** Set the prefix of the command (i.e {prefix}.{command}) to a given prefix. If the command has a
 * module style prefx it gets replaced with the new prefix. If it doesn't, we prepend the prefix to
 * the command. */
void MRCommand_SetPrefix(MRCommand *cmd, const char *newPrefix) {

  char *suffix = strchr(cmd->args[0], '.');
  if (!suffix) {
    suffix = cmd->args[0];
  } else {
    suffix++;
  }

  char *buf = NULL;
  asprintf(&buf, "%s.%s", newPrefix, suffix);
  MRCommand_ReplaceArgNoDup(cmd, 0, buf);
  _getCommandConfId(cmd);
}
void MRCommand_ReplaceArgNoDup(MRCommand *cmd, int index, const char *newArg) {
  if (index < 0 || index >= cmd->num) {
    return;
  }
  char *tmp = cmd->args[index];
  cmd->args[index] = (char *)newArg;
  free(tmp);

  // if we've replaced the first argument, we need to reconfigure the command
  if (index == 0) {
    _getCommandConfId(cmd);
  }
}
void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg) {
  MRCommand_ReplaceArgNoDup(cmd, index, strdup(newArg));
}

MRCommandFlags MRCommand_GetFlags(MRCommand *cmd) {
  if (cmd->id < 0) return 0;
  return __commandConfig[cmd->id].flags;
}

MRCommandGenerator* MRCommand_GetCommandGenerator(MRCommand *cmd) {
  if (cmd->id < 0) {
    return NULL;  // default
  }

  return __commandConfig[cmd->id].commandGenerator;
}

int MRCommand_GetShardingKey(MRCommand *cmd) {
  if (cmd->id < 0) {
    return 1;  // default
  }

  return __commandConfig[cmd->id].keyPos;
}

int MRCommand_GetPartitioningKey(MRCommand *cmd) {
  if (cmd->id < 0) {
    return 1;  // default
  }

  return __commandConfig[cmd->id].partitionKeyPos;
}

/* Return 1 if the command should not be sharded (i.e a coordination command or system command) */
int MRCommand_IsUnsharded(MRCommand *cmd) {
  if (cmd->id < 0) {
    return 0;  // default
  }
  return __commandConfig[cmd->id].keyPos <= 0;
}

void MRCommand_Print(MRCommand *cmd) {

  for (int i = 0; i < cmd->num; i++) {
    printf("%s ", cmd->args[i]);
  }
  printf("\n");
}

void MRCommand_FPrint(FILE *fd, MRCommand *cmd) {

  for (int i = 0; i < cmd->num; i++) {
    fprintf(fd, "%s ", cmd->args[i]);
  }
  fprintf(fd, "\n");
}
