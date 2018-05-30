#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "search_cluster.h"
#include "partition.h"

SearchCluster NewSearchCluster(size_t size, const char **table, size_t tableSize) {
  SearchCluster ret = (SearchCluster){.size = size};
  PartitionCtx_Init(&ret.part, size, table, tableSize);
  return ret;
}

SearchCluster __searchCluster;

SearchCluster *GetSearchCluster() {
  return &__searchCluster;
}

void InitGlobalSearchCluster(size_t size, const char **table, size_t tableSize) {
  __searchCluster = NewSearchCluster(size, table, tableSize);
}

inline int SearchCluster_Ready(SearchCluster *sc) {
  return sc != NULL && sc->size != 0 && sc->part.table != NULL;
}

int SearchCluster_RewriteCommandArg(SearchCluster *sc, MRCommand *cmd, int partitionKey, int arg) {

  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(sc)) return 0;

  if (arg < 0 || arg >= cmd->num || partitionKey >= cmd->num) {
    return 0;
  }

  // the partition arg is the arg which we select the partition on
  char *partitionArg = cmd->args[partitionKey];
  // the sharding arg is the arg that we will add the partition tag to
  char *rewriteArg = cmd->args[arg];
  char *tagged;

  size_t part = PartitionForKey(&sc->part, partitionArg, strlen(partitionArg));
  asprintf(&tagged, "%s{%s}", rewriteArg, PartitionTag(&sc->part, part));
  MRCommand_ReplaceArgNoDup(cmd, arg, tagged);

  return 1;
}

int SearchCluster_RewriteCommand(SearchCluster *sc, MRCommand *cmd, int partitionKey) {
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(sc)) return 0;

  int sk = -1;
  if ((sk = MRCommand_GetShardingKey(cmd)) >= 0) {
    if (partitionKey < 0 || partitionKey >= cmd->num || sk >= cmd->num) {
      return 0;
    }
    // the partition arg is the arg which we select the partition on
    char *partitionArg = cmd->args[partitionKey];
    // the sharding arg is the arg that we will add the partition tag to
    char *shardingArg = cmd->args[sk];

    char *tagged;

    size_t part = PartitionForKey(&sc->part, partitionArg, strlen(partitionArg));
    asprintf(&tagged, "%s{%s}", shardingArg, PartitionTag(&sc->part, part));
    MRCommand_ReplaceArgNoDup(cmd, sk, tagged);
  }
  return 1;
}
/* Get the next multiplexed command from the iterator. Return 1 if we are not done, else 0 */
int SCCommandMuxIterator_Next(void *ctx, MRCommand *cmd) {
  SCCommandMuxIterator *it = ctx;
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(it->cluster)) return 0;

  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);
  if (it->keyOffset >= 0 && it->keyOffset < it->cmd->num) {
    char *arg = cmd->args[it->keyOffset];
    char *tagged;
    asprintf(&tagged, "%s{%s}", arg, PartitionTag(&it->cluster->part, it->offset++));
    // printf("tagged: %s\n", tagged);
    MRCommand_ReplaceArgNoDup(cmd, it->keyOffset, tagged);
  }
  // MRCommand_Print(cmd);

  return 1;
}

/* Return the size of the command generator */
size_t SCCommandMuxIterator_Len(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  return it->cluster->size;
}

void SCCommandMuxIterator_Free(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  if (it->cmd) MRCommand_Free(it->cmd);
  it->cmd = NULL;
  free(it);
}

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
MRCommandGenerator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd) {

  SCCommandMuxIterator *mux = malloc(sizeof(SCCommandMuxIterator));
  *mux = (SCCommandMuxIterator){
      .cluster = c, .cmd = cmd, .keyOffset = MRCommand_GetShardingKey(cmd), .offset = 0};

  return (MRCommandGenerator){.Next = SCCommandMuxIterator_Next,
                              .Free = SCCommandMuxIterator_Free,
                              .Len = SCCommandMuxIterator_Len,
                              .ctx = mux};
}

/* Make sure that the cluster either has a size or updates its size from the topology when updated.
 * If the user did not define the number of partitions, we just take the number of shards in the
 * first topology update and get a fix on that */
void SearchCluster_EnsureSize(RedisModuleCtx *ctx, SearchCluster *c, MRClusterTopology *topo) {
  // If the cluster doesn't have a size yet - set the partition number aligned to the shard number
  if (c->size == 0 && MRClusterTopology_IsValid(topo)) {
    RedisModule_Log(ctx, "notice", "Setting number of partitions to %d", topo->numShards);
    c->size = topo->numShards;
    PartitionCtx_SetSize(&c->part, topo->numShards);
  }
}