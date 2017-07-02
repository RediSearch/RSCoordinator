#include <time.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "periodic.h"
typedef struct RMUtilTimer {
  RMutilTimerFunc cb;
  RedisModuleCtx *redisCtx;
  void *privdata;
  struct timespec interval;
  pthread_t thread;
  pthread_mutex_t lock;
  pthread_cond_t cond;
} RMUtilTimer;

static struct timespec timespecAdd(struct timespec *a, struct timespec *b) {
  struct timespec ret;
  ret.tv_sec = a->tv_sec + b->tv_sec;

  long long ns = a->tv_nsec + b->tv_nsec;
  ret.tv_sec += ns / 1000000000;
  ret.tv_nsec = ns % 1000000000;
  return ret;
}

static void *rmutilTimer_Loop(void *ctx) {
  RMUtilTimer *tm = ctx;

  int rc = ETIMEDOUT;
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  pthread_mutex_lock(&tm->lock);
  while (rc != 0) {
    struct timespec timeout = timespecAdd(&ts, &tm->interval);
    if ((rc = pthread_cond_timedwait(&tm->cond, &tm->lock, &timeout)) == ETIMEDOUT) {
      clock_gettime(CLOCK_REALTIME, &ts);

      tm->cb(tm->redisCtx, tm->privdata);
    }
  }
  //  RedisModule_Log(tm->redisCtx, "notice", "Timer cancelled");

  return NULL;
}

RMUtilTimer *RMUtil_NewPeriodicTimer(RMutilTimerFunc cb, void *privdata, struct timespec interval) {
  RMUtilTimer *ret = malloc(sizeof(*ret));
  *ret = (RMUtilTimer){
      .privdata = privdata,
      .redisCtx = RedisModule_GetThreadSafeContext ? RedisModule_GetThreadSafeContext(NULL) : NULL,
      .interval = interval,
      .cb = cb,
  };
  pthread_cond_init(&ret->cond, NULL);
  pthread_mutex_init(&ret->lock, NULL);

  pthread_create(&ret->thread, NULL, rmutilTimer_Loop, ret);
  return ret;
}

int RMUtilTimer_Stop(RMUtilTimer *t) {
  int rc;
  if (0 == (rc = pthread_cond_signal(&t->cond))) {
    rc = pthread_join(t->thread, NULL);
  }
  return rc;
}

void RMUtilTimer_Free(RMUtilTimer *t) {
  free(t);
}
