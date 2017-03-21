#include "config.h"
#include <string.h>

SearchClusterConfig clusterConfig;
/* Load the configuration from the module arguments.
 * Argument format:
 *  {num_partitions} {cluster type} */
int ParseConfig(SearchClusterConfig *conf, RedisModuleString **argv, int argc) {
  *conf = DEFAULT_CLUSTER_CONFIG;

  if (argc != 2) {
    return REDISMODULE_ERR;
  }

  /* Parse the partition number */
  long long numPartitions = 0;
  if (RedisModule_StringToLongLong(argv[0], &numPartitions) == REDISMODULE_ERR ||
      numPartitions <= 0) {
    printf("Invalid num partitions");
    return REDISMODULE_ERR;
  }
  conf->numPartitions = numPartitions;

  /* Parse the cluster type and make sure it's valid */
  const char *clusterType = RedisModule_StringPtrLen(argv[1], NULL);
  const char *clusterTypes[] = {CLUSTER_TYPE_OSS, CLUSTER_TYPE_RLABS, CLUSTER_TYPE_STATIC};
  int found = 0;
  for (int i = 0; !found && i < sizeof(clusterTypes) / sizeof(const char *); i++) {
    if (!strcmp(clusterType, clusterTypes[i])) {
      conf->clusterType = clusterTypes[i];
      found = 1;
      break;
    }
  }
  if (!found) {
    printf("Invalid cluster type %s\n", clusterType);
  }

  return found ? REDISMODULE_OK : REDISMODULE_ERR;
}
