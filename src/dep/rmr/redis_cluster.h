#ifndef __RMR_REDIS_CLUSTER_H__
#define __RMR_REDIS_CLUSTER_H__

#include "cluster.h"

MRTopologyProvider NewRedisClusterTopologyProvider(MREndpoint *localEndpoint);

MRClusterTopology *RedisCluster_GetTopology(void *p, void *rc);

#endif