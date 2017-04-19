#ifndef __SERACH_CLUSTER_H__
#define __SERACH_CLUSTER_H__

#include <stdint.h>
#include "dep/rmr/command.h"

/* A partitioner is an abstract interface that takes command keys and tags them according to a
 * sharding function matching the cluster's sharding function.
 * Using a partitioner we can paste sharding tags onto redis arguments to make sure they reach
 * specific shards in the cluster, thus reducing the number of shards in the cluster to well bellow
 * 16384 or 4096.
  */
typedef struct {
  void *ctx;
  /* Get the partition number for a given key with a given len */
  size_t (*PartitionForKey)(void *ctx, const char *key, size_t len);
  /* Get the partitioning tag string given a  partition number */
  const char *(*PartitionTag)(void *ctx, size_t partition);
  /* Free the paritioner */
  void (*Free)(void *ctx);
} Partitioner;

/* Create a SimplePartitioner, that using a table of strings per redis hash slot, tries to balance
 * the spread of partitions across redis shards */
Partitioner NewSimplePartitioner(size_t numPartitions, const char **table, size_t tableSize);

/* A search cluster contains the configuations for partitioning and multiplexing commands */
typedef struct {
  size_t size;
  Partitioner part;
} SearchCluster;

/* Create a search cluster with a given number of partitions (size) and a partitioner.
 * TODO: This whole object is a bit redundant and adds nothing on top of the partitioner. Consider
 * consolidating the two  */
SearchCluster NewSearchCluster(size_t size, Partitioner pt);

/* A command generator that multiplexes a command across multiple partitions by tagging it */
typedef struct {
  MRCommand *cmd;
  int keyOffset;
  size_t offset;
  SearchCluster *cluster;
} SCCommandMuxIterator;

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
#endif