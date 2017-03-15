#ifndef __SERACH_CLUSTER_H__
#define __SERACH_CLUSTER_H__
#include <stdint.h>
#include "command.h"

typedef struct {
  void *ctx;
  size_t (*PartitionForKey)(void *ctx, const char *key, size_t len);
  const char *(*PartitionTag)(void *ctx, size_t partition);
  void (*Free)(void *ctx);
} Partitioner;

Partitioner NewSimplePartitioner(size_t size);

typedef struct {
  size_t size;
  Partitioner part;
} SearchCluster;

SearchCluster NewSearchCluster(size_t size, Partitioner pt);

typedef struct {
  MRCommand *cmd;
  int keyOffset;
  size_t offset;
  SearchCluster *cluster;
} SCCommandMuxIterator;

/* Multiplex a command to the cluster using an iterator that will yield a multiplexed command per
 * iteration, based on the original command */
SCCommandMuxIterator SearchCluster_MultiplexCommand(SearchCluster *c, MRCommand *cmd,
                                                    int keyOffset);

/* Get the next multiplexed command from the iterator. Return 1 if we are not done, else 0 */
int SCCommandMuxIterator_Next(SCCommandMuxIterator *it, MRCommand *cmd);

#endif