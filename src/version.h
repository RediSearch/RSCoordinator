#ifndef RSCOORDINATOR_VERSION_H_
#define RSCOORDINATOR_VERSION_H_
#include "dep/RediSearch/src/version.h"

#define RSCOORDINATOR_VERSION_MAJOR 99
#define RSCOORDINATOR_VERSION_MINOR 99
#define RSCOORDINATOR_VERSION_PATCH 99

// convert semver to incremental number as expected by redis
#define RSCOORDINATOR_VERSION                                                \
  (RSCOORDINATOR_VERSION_MAJOR * 10000 + RSCOORDINATOR_VERSION_MINOR * 100 + \
   RSCOORDINATOR_VERSION_PATCH)

#define RSCOORDINATOR_MODULE_NAME REDISEARCH_MODULE_NAME

#endif
