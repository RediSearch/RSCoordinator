#include <string.h>
#include <stdlib.h>

#include "config.h"
#include <rmutil/util.h>
#include <rmutil/strings.h>
#include "dep/rmr/endpoint.h"
#include "dep/rmr/hiredis/hiredis.h"

SearchClusterConfig clusterConfig;
/* Load the configuration from the module arguments.
 * Argument format:
 *  PARTITIONS {num_partitions} TYPE {cluster type} ENDPOINT {[password@]host:port}
 */
int ParseConfig(SearchClusterConfig *conf, RedisModuleCtx *ctx, RedisModuleString **argv,
                int argc) {
  *conf = DEFAULT_CLUSTER_CONFIG;

  if (argc < 2) {
    return REDISMODULE_ERR;
  }

  /* Parse the partition number */
  long long numPartitions = 0;
  int pi = -1;
  if ((pi = RMUtil_ArgIndex("PARTITIONS", argv, argc)) >= 0) {
    // Allow PARTITIONS AUTO (which is similar to 0)
    if (pi < argc - 1 && RMUtil_StringEqualsC(argv[pi + 1], "AUTO")) {
      numPartitions = 0;
    } else {
      // Parse number of partitions manually
      if (RMUtil_ParseArgsAfter("PARTITIONS", argv, argc, "l", &numPartitions) == REDISMODULE_ERR ||
          numPartitions < 0) {
        RedisModule_Log(ctx, "warning", "Invalid num partitions %d", numPartitions);
        return REDISMODULE_ERR;
      }
    }
  }
  conf->numPartitions = numPartitions;
  conf->type = DetectClusterType();

  /* Read the query timeout */
  if (argc >= 2 && RMUtil_ArgIndex("TIMEOUT", argv, argc) >= 0) {
    long long to = 500;
    RMUtil_ParseArgsAfter("TIMEOUT", argv, argc, "l", &to);
    if (to > 0) {
      conf->timeoutMS = to;
    }
  }
  return REDISMODULE_OK;
}

/* Detect the cluster type, by trying to see if we are running inside RLEC.
 * If we cannot determine, we return OSS type anyway
 */
MRClusterType DetectClusterType() {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);

  RedisModuleCallReply *r = RedisModule_Call(ctx, "INFO", "c", "SERVER");
  MRClusterType ret = ClusterType_RedisOSS;

  if (r && RedisModule_CallReplyType(r) == REDISMODULE_REPLY_STRING) {
    size_t len;
    // INFO SERVER should contain the term rlec_version in it if we are inside an RLEC shard

    const char *str = RedisModule_CallReplyStringPtr(r, &len);
    if (str) {

      if (memmem(str, len, "rlec_version", strlen("rlec_version")) != NULL) {
        ret = ClusterType_RedisLabs;
      }
    }
    RedisModule_FreeCallReply(r);
  }
  // RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_FreeThreadSafeContext(ctx);
  return ret;
}