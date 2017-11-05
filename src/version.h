#ifndef RSCOORDINATOR_VERSION_H_
#define RSCOORDINATOR_VERSION_H_

#define RSCOODINATOR_VERSION_MAJOR 0
#define RSCOODINATOR_VERSION_MINOR 91
#define RSCOODINATOR_VERSION_PATCH 0

// convert semver to incremental number as expected by redis
#define RSCOODINATOR_VERSION                                               \
  (RSCOODINATOR_VERSION_MAJOR * 10000 + RSCOODINATOR_VERSION_MINOR * 100 + \
   RSCOODINATOR_VERSION_PATCH)

#endif
