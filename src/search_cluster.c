#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "search_cluster.h"
#include "fnv.h"

typedef struct {
  size_t size;
  char **table;
} SimplePartitioner;

size_t SP_PartitionForKey(void *ctx, const char *key, size_t len) {
  SimplePartitioner *sp = ctx;
  return fnv_32a_buf(key, len, 0) % sp->size;
}

void SP_Free(void *ctx) {
  SimplePartitioner *sp = ctx;
  for (size_t i = 0; i < sp->size; i++) {
    free(sp->table[i]);
  }
  free(sp->table);
  free(sp);
}

const char *SP_PartitionTag(void *ctx, size_t partition) {
  SimplePartitioner *sp = ctx;

  if (partition > sp->size) {
    return NULL;
  }

  return sp->table[partition];
}

Partitioner NewSimplePartitioner(size_t size) {

  SimplePartitioner *sp = malloc(sizeof(SimplePartitioner));
  *sp = (SimplePartitioner){.size = size, .table = calloc(size, sizeof(const char *))};
  for (size_t i = 0; i < size; i++) {
    sp->table[i] = malloc(8);
    snprintf(sp->table[i], 8, "%02zx", i);
  }

  return (Partitioner){.ctx = sp,
                       .PartitionForKey = SP_PartitionForKey,
                       .PartitionTag = SP_PartitionTag,
                       .Free = SP_Free};
}

SearchCluster NewSearchCluster(size_t size, Partitioner pt) {
  return (SearchCluster){.size = size, .part = pt};
}

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
SCCommandMuxIterator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd,
                                                    int keyOffset) {
  return (SCCommandMuxIterator){.cluster = c, .cmd = cmd, .keyOffset = keyOffset, .offset = 0};
}

/* Get the next multiplexed command from the iterator. Return 1 if we are not done, else 0 */
int SCCommandMuxIterator_Next(SCCommandMuxIterator *it, MRCommand *cmd) {
  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);
  char *arg = cmd->args[it->keyOffset];
  char *tagged = malloc(strlen(arg) + 16);
  sprintf(tagged, "%s{%s}", arg,
          it->cluster->part.PartitionTag(it->cluster->part.ctx, it->offset++));
  free(arg);
  cmd->args[it->keyOffset] = tagged;
  return 1;
}
