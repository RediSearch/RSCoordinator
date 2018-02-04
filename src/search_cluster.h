#ifndef __SERACH_CLUSTER_H__
#define __SERACH_CLUSTER_H__

#include <stdint.h>
#include "dep/rmr/command.h"
#include "dep/rmr/cluster.h"
#include "partition.h"

/* A search cluster contains the configuations for partitioning and multiplexing commands */
typedef struct {
  size_t size;
  PartitionCtx part;

} SearchCluster;

/* Create a search cluster with a given number of partitions (size) and a partitioner.
 * TODO: This whole object is a bit redundant and adds nothing on top of the partitioner. Consider
 * consolidating the two  */
SearchCluster NewSearchCluster(size_t size, const char **table, size_t tableSize);

/* A command generator that multiplexes a command across multiple partitions by tagging it */
typedef struct {
  MRCommand *cmd;
  int keyOffset;
  size_t offset;
  SearchCluster *cluster;
} SCCommandMuxIterator;

int SearchCluster_Ready(SearchCluster *sc);

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
MRCommandGenerator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd);

/* Rewrite a command by tagging its sharding key, using its partitioning key (which may or may not
 * be the same key) */
int SearchCluster_RewriteCommand(SearchCluster *c, MRCommand *cmd, int partitionKey);

/* Rewrite a specific argument in a command by tagging it using the partition key, arg is the
 * index
 * of the argument being tagged, and it may be the paritioning key itself */
int SearchCluster_RewriteCommandArg(SearchCluster *c, MRCommand *cmd, int partitionKey, int arg);

/* Make sure that if the cluster is unaware of its sizing, it will take the size from the topology
 */
void SearchCluster_EnsureSize(RedisModuleCtx *ctx, SearchCluster *c, MRClusterTopology *topo);
#endif