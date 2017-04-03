#ifndef __MR_REDISE_H__
#define __MR_REDISE_H__

#include "cluster.h"

typedef struct { MRClusterTopology *topology; } RedisEnterpriseCtx;
/* Redis^e cluster topology provider */

MRClusterTopology *RedisEnterprise_GetTopology(void *p);

int RedisEnterprise_ParseTopology(RedisEnterpriseCtx *redise, RedisModuleCtx *ctx,
                                  RedisModuleString **argv, int argc);

#endif