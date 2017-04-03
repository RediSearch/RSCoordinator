#ifndef __MR_REDISE_H__
#define __MR_REDISE_H__

#include "cluster.h"

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                 int argc);

#endif
