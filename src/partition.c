#include "partition.h"
#include "fnv.h"
#include <stdio.h>

size_t PartitionForKey(PartitionCtx *ctx, const char *key, size_t len) {
  return fnv_32a_buf((void *)key, len, 0) % ctx->size;
}

const char *PartitionTag(PartitionCtx *ctx, size_t partition) {

  if (partition > ctx->size) {
    return NULL;
  }

  size_t step = ctx->tableSize / ctx->size;
  // printf("parition %d, index %d\n", partition, partition * (sp->tableSize / sp->size));
  return ctx->table[((partition + 1) * step - 1) % ctx->tableSize];
}

void PartitionCtx_Init(PartitionCtx *ctx, size_t numPartitions, const char **table,
                       size_t tableSize) {
  // fprintf(stderr, "Initializing partition to table %p size %d\n", table, tableSize);
  ctx->size = numPartitions;
  ctx->table = table;
  ctx->tableSize = tableSize;
}

void PartitionCtx_SetSlotTable(PartitionCtx *ctx, const char **table, size_t tableSize) {
  ctx->table = table;
  ctx->tableSize = tableSize;
}

/* Set the number of partitions in this partition context */
void PartitionCtx_SetSize(PartitionCtx *ctx, size_t size) {
  ctx->size = size;
}