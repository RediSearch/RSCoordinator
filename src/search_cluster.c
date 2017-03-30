#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "search_cluster.h"
#include "crc16_tags.h"
#include "fnv.h"

typedef struct {
  size_t size;
  const char **table;
  size_t tableSize;
} SimplePartitioner;

size_t SP_PartitionForKey(void *ctx, const char *key, size_t len) {
  SimplePartitioner *sp = ctx;
  return fnv_32a_buf((void *)key, len, 0) % sp->size;
}

void SP_Free(void *ctx) {
  SimplePartitioner *sp = ctx;
  
  free(sp);
}

const char *SP_PartitionTag(void *ctx, size_t partition) {
  SimplePartitioner *sp = ctx;

  if (partition > sp->size) {
    return NULL;
  }

  // index = partition * (sp->tableSize / sp->size);
  return sp->table[partition * (sp->tableSize / sp->size)];
}

Partitioner NewSimplePartitioner(size_t numPartitions, const char **table, size_t tableSize) {

  SimplePartitioner *sp = malloc(sizeof(SimplePartitioner));
  *sp = (SimplePartitioner){.size = numPartitions,
                            .table = calloc(numPartitions, sizeof(const char *))};
  sp->table = table;
  sp->tableSize = tableSize;

  return (Partitioner){.ctx = sp,
                       .PartitionForKey = SP_PartitionForKey,
                       .PartitionTag = SP_PartitionTag,
                       .Free = SP_Free};
}

SearchCluster NewSearchCluster(size_t size, Partitioner pt) {
  return (SearchCluster){.size = size, .part = pt};
}

int SearchCluster_RewriteCommandArg(SearchCluster *sc, MRCommand *cmd, int partitionKey, int arg) {

  if (arg < 0) {
    return 0;
  }

  // the partition arg is the arg which we select the partition on
  char *partitionArg = cmd->args[partitionKey];
  // the sharding arg is the arg that we will add the partition tag to
  char *rewriteArg = cmd->args[arg];

  char *tagged = malloc(strlen(rewriteArg) + 24);

  size_t part = sc->part.PartitionForKey(sc->part.ctx, partitionArg, strlen(partitionArg));
  snprintf(tagged, strlen(rewriteArg) + 24, "%s{%s}", rewriteArg,
           sc->part.PartitionTag(sc->part.ctx, part));
  MRCommand_ReplaceArg(cmd, arg, tagged);

  return 1;
}

int SearchCluster_RewriteCommand(SearchCluster *sc, MRCommand *cmd, int partitionKey) {

  int sk = -1;
  if ((sk = MRCommand_GetShardingKey(cmd)) >= 0) {
    // the partition arg is the arg which we select the partition on
    char *partitionArg = cmd->args[partitionKey];
    // the sharding arg is the arg that we will add the partition tag to
    char *shardingArg = cmd->args[sk];

    char *tagged = malloc(strlen(shardingArg) + 16);

    size_t part = sc->part.PartitionForKey(sc->part.ctx, partitionArg, strlen(partitionArg));
    sprintf(tagged, "%s{%s}", shardingArg, sc->part.PartitionTag(sc->part.ctx, part));
    MRCommand_ReplaceArg(cmd, sk, tagged);
  }
  return 1;
}
/* Get the next multiplexed command from the iterator. Return 1 if we are not done, else 0 */
int SCCommandMuxIterator_Next(void *ctx, MRCommand *cmd) {
  SCCommandMuxIterator *it = ctx;
  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);
  if (it->keyOffset >= 0) {
    char *arg = cmd->args[it->keyOffset];
    char *tagged = malloc(strlen(arg) + 16);
    sprintf(tagged, "%s{%s}", arg,
            it->cluster->part.PartitionTag(it->cluster->part.ctx, it->offset++));
    free(arg);
    cmd->args[it->keyOffset] = tagged;
  }

  return 1;
}

/* Return the size of the command generator */
size_t SCCommandMuxIterator_Len(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  return it->cluster->size;
}

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
MRCommandGenerator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd, int keyOffset) {

  SCCommandMuxIterator *mux = malloc(sizeof(SCCommandMuxIterator));
  *mux = (SCCommandMuxIterator){.cluster = c, .cmd = cmd, .keyOffset = keyOffset, .offset = 0};

  return (MRCommandGenerator){
      .Next = SCCommandMuxIterator_Next, .Free = free, .Len = SCCommandMuxIterator_Len, .ctx = mux};
}
