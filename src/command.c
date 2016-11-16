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
    MRCommand cmd = (MRCommand){.num= argc, .args = calloc(argc, sizeof(char **))};
    for (int i = 0; i < argc; i++) {
        cmd.args[i] = strdup(argv[i]);
    }
    return cmd;
}

MRCommand MR_NewCommand(int argc, ...) {
    MRCommand cmd = (MRCommand){.num= argc, .args = calloc(argc, sizeof(char **))};
    va_list ap;
    va_start(ap, argc);
    for (int i = 0; i < argc; i++) {
        cmd.args[i] = strdup(va_arg(ap, const char *));
    }
    va_end(ap);

    return cmd;
}

MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv) {
    MRCommand cmd = (MRCommand){.num= argc, .args = calloc(argc, sizeof(char **))};
    for (int i = 0; i < argc; i++) {
        cmd.args[i] = strdup(RedisModule_StringPtrLen(argv[i], NULL));
    }
    return cmd;
}

