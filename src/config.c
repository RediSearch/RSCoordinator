#include <string.h>
#include <stdlib.h>

#include "config.h"
#include "dep/rmutil/util.h"
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

  for (int i = 0; i < argc; i++) {
    printf("%s ", RedisModule_StringPtrLen(argv[i], NULL));
  }
  printf("\n");

  /* Parse the partition number */
  long long numPartitions = 0;
  RMUtil_ParseArgsAfter("PARTITIONS", argv, argc, "l", &numPartitions);
  if (numPartitions <= 0) {
    RedisModule_Log(ctx, "warning", "Invalid num partitions %d", numPartitions);
    return REDISMODULE_ERR;
  }
  conf->numPartitions = numPartitions;

  const char *clusterType = NULL;
  RMUtil_ParseArgsAfter("TYPE", argv, argc, "c", &clusterType);

  int found = 0;
  if (clusterType) {
    /* Parse the cluster type and make sure it's valid */
    const char *clusterTypes[] = {[ClusterType_RedisOSS] = CLUSTER_TYPE_OSS,
                                  [ClusterType_RedisLabs] = CLUSTER_TYPE_RLABS};

    for (int i = 0; !found && i < sizeof(clusterTypes) / sizeof(const char *); i++) {
      if (!strcmp(clusterType, clusterTypes[i])) {
        conf->type = i;
        found = 1;
        break;
      }
    }
  }
  // the cluster type was not found
  if (!found) {
    RedisModule_Log(ctx, "error", "Invalid cluster type %s\n", clusterType);
    return REDISMODULE_ERR;
  }

  // Parse the endpoint
  char *ep = NULL;
  RMUtil_ParseArgsAfter("ENDPOINT", argv, argc, "c", &ep);
  if (ep) {
    MREndpoint endp;
    if (MREndpoint_Parse(ep, &endp) == REDIS_ERR) {
      RedisModule_Log(ctx, "error", "Invalid endpoint %s\n", ep);
      return REDISMODULE_ERR;
    }
    conf->myEndpoint = malloc(sizeof(MREndpoint));
    *conf->myEndpoint = endp;
    RedisModule_Log(ctx, "notice", "Our endpoint: %s@%s:%d", endp.auth ? endp.auth : "", endp.host,
                    endp.port);
  }

  return REDISMODULE_OK;
}
