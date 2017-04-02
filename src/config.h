#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "redismodule.h"

typedef enum { ClusterType_RedisOSS = 0, ClusterType_RedisLabs = 1 } MRClusterType;

typedef struct {
  size_t numPartitions;
  MRClusterType type;
} SearchClusterConfig;

extern SearchClusterConfig clusterConfig;

#define CLUSTER_TYPE_OSS "redis_oss"
#define CLUSTER_TYPE_RLABS "redislabs"

#define DEFAULT_CLUSTER_CONFIG                       \
  (SearchClusterConfig) {                            \
    .numPartitions = 1, .type = ClusterType_RedisOSS \
  }

/* Load the configuration from the module arguments.
 * Argument format: PARTITIONS {num_partitions} TYPE {cluster type} ENDPOINT {[password@]host:port}
 */
int ParseConfig(SearchClusterConfig *conf, RedisModuleString **argv, int argc);

#endif
