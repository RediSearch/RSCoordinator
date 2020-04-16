#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "search_cluster.h"
#include "partition.h"
#include "alias.h"

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

char *writeTaggedId(const char *key, size_t keyLen, const char *tag, size_t tagLen,
                    size_t *taggedLen) {
  size_t total = keyLen + tagLen + 3;  // +3 because of '{', '}', and NUL
  char *tagged = malloc(total);
  tagged[total - 1] = 0;
  if (taggedLen) {
    *taggedLen = total - 1;
  }

  // key{tag}

  char *pos = tagged;
  memcpy(pos, key, keyLen);
  pos += keyLen;
  *(pos++) = '{';

  memcpy(pos, tag, tagLen);
  pos += tagLen;
  *(pos++) = '}';

  // printf("TaggedID: %.*s\n", (int)*taggedLen, tagged);
  return tagged;
}

/**
 * Rewrite a command for a given partition.
 * @param sc the cluster
 * @param cmd the command to rewrite
 * @param dstArg the index within the command that contains the key to rewrite
 * @param partition the partition to use for tagging
 */
static void SearchCluster_RewriteForPartition(SearchCluster *sc, MRCommand *cmd, int dstArg,
                                              size_t partition) {
  size_t narg;
  const char *arg = MRCommand_ArgStringPtrLen(cmd, dstArg, &narg);
  const char *partTag = PartitionTag(&sc->part, partition);
  size_t taggedLen;
  char *tagged = writeTaggedId(arg, narg, partTag, strlen(partTag), &taggedLen);
  MRCommand_ReplaceArgNoDup(cmd, dstArg, tagged, taggedLen);
}

int SearchCluster_RewriteCommandArg(SearchCluster *sc, MRCommand *cmd, int partitionKey, int arg) {

  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(sc)) return 0;

  if (arg < 0 || arg >= cmd->num || partitionKey >= cmd->num) {
    return 0;
  }

  // the partition arg is the arg which we select the partition on
  const char *partitionArg, *rewriteArg;
  size_t partitionLen, rewriteLen;

  partitionArg = MRCommand_ArgStringPtrLen(cmd, partitionKey, &partitionLen);

  size_t part = PartitionForKey(&sc->part, partitionArg, partitionLen);
  SearchCluster_RewriteForPartition(sc, cmd, arg, part);
  return 1;
}

static const char *getUntaggedId(const char *id, size_t *outlen) {
  const char *openBrace = rindex(id, '{');
  if (openBrace) {
    *outlen = openBrace - id;
  } else {
    *outlen = strlen(id);
  }
  return id;
}

static const char *lookupAlias(const char *orig, size_t *len) {
  IndexSpec *sp = IndexAlias_Get(orig);
  if (!sp) {
    *len = strlen(orig);
    return orig;
  }
  return getUntaggedId(sp->name, len);
}

int SearchCluster_RewriteCommand(SearchCluster *sc, MRCommand *cmd, int partIdx) {
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(sc)) return 0;

  int sk = -1;
  if ((sk = MRCommand_GetShardingKey(cmd)) >= 0) {
    if (partIdx < 0 || partIdx >= cmd->num || sk >= cmd->num) {
      return 0;
    }

    // printf("ShardKey: %d. Before rewrite: ", sk);
    // MRCommand_Print(cmd);

    size_t partLen = 0, targetLen = 0, taggedLen;

    // the partition arg is the arg which we select the partition on
    const char *partStr = MRCommand_ArgStringPtrLen(cmd, partIdx, &partLen);

    // the sharding arg is the arg that we will add the partition tag to
    const char *target = MRCommand_ArgStringPtrLen(cmd, sk, &targetLen);

    size_t partId = PartitionForKey(&sc->part, partStr, partLen);
    const char *tag = PartitionTag(&sc->part, partId);
    if (MRCommand_GetFlags(cmd) & MRCommand_Aliased) {
      // 1:1 partition mapping
      IndexSpec *spec = IndexAlias_Get(target);
      if (spec) {
        target = spec->name;
        targetLen = rindex(spec->name, '{') - target;
      }
    }

    char *tagged = writeTaggedId(target, targetLen, tag, strlen(tag), &taggedLen);
    MRCommand_ReplaceArgNoDup(cmd, sk, tagged, taggedLen);

    // printf("After rewrite: ");
    // MRCommand_Print(cmd);
  }
  return 1;
}

int SearchCluster_RewriteCommandToFirstPartition(SearchCluster *sc, MRCommand *cmd) {
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(sc)) return 0;

  int sk = MRCommand_GetShardingKey(cmd);
  if (sk < 0) {
    return 1;
  } else if (sk >= cmd->num) {
    return 0;
  }

  size_t keylen = 0;
  const char *key = MRCommand_ArgStringPtrLen(cmd, sk, &keylen);
  if (MRCommand_GetFlags(cmd) & MRCommand_Aliased) {
    key = lookupAlias(key, &keylen);
  }

  size_t taggedLen = 0;
  const char *tag = PartitionTag(&sc->part, 0);
  char *tagged = writeTaggedId(key, keylen, tag, strlen(tag), &taggedLen);
  MRCommand_ReplaceArgNoDup(cmd, sk, tagged, taggedLen);
  return 1;
}

/* Get the next multiplexed command for spellcheck command. Return 1 if we are not done, else 0 */
int SpellCheckMuxIterator_Next(void *ctx, MRCommand *cmd) {
  SCCommandMuxIterator *it = ctx;
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(it->cluster)) return 0;

  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);
  if (it->keyOffset >= 0 && it->keyOffset < it->cmd->num) {
    SearchCluster_RewriteForPartition(it->cluster, cmd, it->keyOffset, it->offset);
  }

  for (size_t i = 0; i < it->cmd->num; ++i) {
    size_t argLen;
    const char *arg = MRCommand_ArgStringPtrLen(it->cmd, i, &argLen);
    if (!strncasecmp("terms", arg, argLen)) {
      if (i + 2 < it->cmd->num) {
        SearchCluster_RewriteForPartition(it->cluster, cmd, i + 2, it->offset);
      }
    }
  }

  // we ask for full score info so we can aggregate correctly
  MRCommand_AppendArgs(cmd, 1, "FULLSCOREINFO");

  ++it->offset;

  return 1;
}

int SCNoPartitionMuxIterator_Next(void *ctx, MRCommand *cmd) {
  SCCommandMuxIterator *it = ctx;
  // make sure we can actually calculate partitioning
  if (!SearchCluster_Ready(it->cluster)) return 0;

  /* at end */
  if (it->offset >= it->cluster->size) {
    return 0;
  }

  *cmd = MRCommand_Copy(it->cmd);
  if (it->keyAlias) {
      MRCommand_ReplaceArg(cmd, it->keyOffset, it->keyAlias, strlen(it->keyAlias));
  }

  cmd->slotToSend = GetSlotByPartition(&it->cluster->part, it->offset++);

  return 1;
}

/* Return the size of the command generator */
size_t SCNoPartitionMuxIterator_Len(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  return it->cluster->size;
}

void SCNoPartitionMuxIterator_Free(void *ctx) {
  SCCommandMuxIterator *it = ctx;
  if (it->cmd) MRCommand_Free(it->cmd);
  it->cmd = NULL;
  free(it->keyAlias);
  free(it);
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
    size_t argLen;
    const char *arg;
    if (it->keyAlias) {
      arg = it->keyAlias;
      argLen = strlen(it->keyAlias);
    } else {
      arg = MRCommand_ArgStringPtrLen(cmd, it->keyOffset, &argLen);
    }
    const char *tag = PartitionTag(&it->cluster->part, it->offset++);

    size_t taggedLen;
    char *tagged = writeTaggedId(arg, argLen, tag, strlen(tag), &taggedLen);
    MRCommand_ReplaceArgNoDup(cmd, it->keyOffset, tagged, taggedLen);
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
  free(it->keyAlias);
  free(it);
}

MRCommandGenerator noPartitionCommandGenerator = {.Next = SCNoPartitionMuxIterator_Next,
                                                  .Free = SCNoPartitionMuxIterator_Free,
                                                  .Len = SCNoPartitionMuxIterator_Len,
                                                  .ctx = NULL};

MRCommandGenerator defaultCommandGenerator = {.Next = SCCommandMuxIterator_Next,
                                              .Free = SCCommandMuxIterator_Free,
                                              .Len = SCCommandMuxIterator_Len,
                                              .ctx = NULL};

MRCommandGenerator spellCheckCommandGenerator = {.Next = SpellCheckMuxIterator_Next,
                                                 .Free = SCCommandMuxIterator_Free,
                                                 .Len = SCCommandMuxIterator_Len,
                                                 .ctx = NULL};

MRCommandGenerator SearchCluster_GetCommandGenerator(SCCommandMuxIterator *mux, MRCommand *cmd) {
  MRCommandGenerator *ptr = MRCommand_GetCommandGenerator(cmd);
  MRCommandGenerator ret;
  if (ptr) {
    ret = *ptr;
  } else {
    ret = noPartitionCommandGenerator;
  }
  ret.ctx = mux;
  return ret;
}

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
MRCommandGenerator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd) {

  SCCommandMuxIterator *mux = malloc(sizeof(SCCommandMuxIterator));
  *mux = (SCCommandMuxIterator){
      .cluster = c, .cmd = cmd, .keyOffset = MRCommand_GetShardingKey(cmd), .offset = 0};
  if (MRCommand_GetFlags(cmd) & MRCommand_Aliased) {
    if (mux->keyOffset > 0 && mux->keyOffset < cmd->num) {
      size_t oldlen = strlen(cmd->strs[mux->keyOffset]);
      size_t newlen = 0;
      const char *target = lookupAlias(cmd->strs[mux->keyOffset], &newlen);
      if (oldlen != newlen) {
        mux->keyAlias = strndup(target, newlen);
      }
    }
  }
  return SearchCluster_GetCommandGenerator(mux, cmd);
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

void SetMyPartition(MRClusterTopology *ct, MRClusterShard *myShard) {
  SearchCluster *c = GetSearchCluster();
  for (size_t i = 0; i < c->size; ++i) {
    int slot = GetSlotByPartition(&c->part, i);
    if (myShard->startSlot <= slot && myShard->endSlot >= slot) {
      c->myPartition = i;
      return;
    }
  }
}
