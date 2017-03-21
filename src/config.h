#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "redismodule.h"

typedef struct {
  size_t numPartitions;
  const char *clusterType;
} SearchClusterConfig;

extern SearchClusterConfig clusterConfig;

#define CLUSTER_TYPE_OSS "redis_oss"
#define CLUSTER_TYPE_STATIC "redis_static"
#define CLUSTER_TYPE_RLABS "redislabs"

#define DEFAULT_CLUSTER_CONFIG                             \
  (SearchClusterConfig) {                                  \
    .numPartitions = 1, .clusterType = CLUSTER_TYPE_STATIC \
  }

/* Load the configuration from the module arguments.
 * Argument format: {num_partitions} {cluster type} */
int ParseConfig(SearchClusterConfig *conf, RedisModuleString **argv, int argc);

#endif
