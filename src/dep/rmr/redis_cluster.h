#ifndef __RMR_REDIS_CLUSTER_H__
#define __RMR_REDIS_CLUSTER_H__

#include "cluster.h"

MRTopologyProvider NewRedisClusterTopologyProvider(void *ctx);

MRClusterTopology *RedisCluster_GetTopology(void *p);

#endif