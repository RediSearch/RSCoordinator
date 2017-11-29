#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "redismodule.h"
#include "dep/rmr/endpoint.h"
#include <string.h>
typedef enum { ClusterType_RedisOSS = 0, ClusterType_RedisLabs = 1 } MRClusterType;

typedef struct {
  size_t numPartitions;
  MRClusterType type;
} SearchClusterConfig;

extern SearchClusterConfig clusterConfig;

#define CLUSTER_TYPE_OSS "redis_oss"
#define CLUSTER_TYPE_RLABS "redislabs"

#define DEFAULT_CLUSTER_CONFIG                      \
  (SearchClusterConfig) {                           \
    .numPartitions = 0, .type = DetectClusterType() \
  }

/* Detect the cluster type, by trying to see if we are running inside RLEC.
 * If we cannot determine, we return OSS type anyway
 */
MRClusterType DetectClusterType();

/* Load the configuration from the module arguments.
 * Argument format: PARTITIONS {num_partitions} ENDPOINT {[password@]host:port}
 */
int ParseConfig(SearchClusterConfig *conf, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif
