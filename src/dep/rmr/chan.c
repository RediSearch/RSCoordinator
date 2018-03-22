#define MR_CHAN_C_
#include <pthread.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>

void *MRCHANNEL_CLOSED = (void *)"MRCHANNEL_CLOSED";

typedef struct chanItem {
  void *ptr;
  struct chanItem *next;
} chanItem;

typedef struct MRChannel {
  chanItem *head;
  chanItem *tail;
  size_t size;
  size_t maxSize;
  int open;
  pthread_mutex_t lock;
  pthread_cond_t cond;
} MRChannel;

#include "chan.h"

MRChannel *MR_NewChannel(size_t max) {
  MRChannel *chan = malloc(sizeof(*chan));
  *chan = (MRChannel){
      .head = NULL,
      .tail = NULL,
      .size = 0,
      .maxSize = max,
      .open = 1,
  };
  pthread_cond_init(&chan->cond, NULL);
  pthread_mutex_init(&chan->lock, NULL);
  return chan;
}

void MRChannel_Free(MRChannel *chan) {

  // TODO: proper drain and stop routine

  pthread_mutex_destroy(&chan->lock);
  pthread_cond_destroy(&chan->cond);
  free(chan);
}

size_t MRChannel_Size(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  size_t ret = chan->size;
  pthread_mutex_unlock(&chan->lock);
  return ret;
}

size_t MRChannel_MaxSize(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  size_t ret = chan->maxSize;
  pthread_mutex_unlock(&chan->lock);
  return ret;
}

int MRChannel_Push(MRChannel *chan, void *ptr) {

  pthread_mutex_lock(&chan->lock);
  int rc = 1;
  if (!chan->open || (chan->maxSize > 0 && chan->size == chan->maxSize)) {
    rc = 0;
    goto end;
  }

  chanItem *item = malloc(sizeof(*item));
  item->next = NULL;
  item->ptr = ptr;
  if (chan->tail) {
    // make it the next of the current tail
    chan->tail->next = item;
    // set a new tail
    chan->tail = item;
  } else {  // no tail means no head - empty queue
    chan->head = chan->tail = item;
  }
  chan->size++;
end:
  pthread_mutex_unlock(&chan->lock);
  // if (rc) {
  if (pthread_cond_broadcast(&chan->cond)) rc = 0;
  //}
  return rc;
}

void *MRChannel_PopWait(MRChannel *chan, int waitMS) {
  struct timeval tv;
  struct timespec ts;
  // calculate sleep time
  if (waitMS > 0) {
    gettimeofday(&tv, NULL);
    tv.tv_usec += waitMS * 1000;
    if (tv.tv_usec > 1000000) {
      tv.tv_sec += tv.tv_usec / 1000000;
      tv.tv_usec = tv.tv_usec % 1000000;
    }
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
  }
  pthread_mutex_lock(&chan->lock);

  if (chan->size == 0) {
    if (!chan->open) {
      pthread_mutex_unlock(&chan->lock);
      return MRCHANNEL_CLOSED;
    }
    if (waitMS) {
      // exit on timeout
      if (pthread_cond_timedwait(&chan->cond, &chan->lock, &ts) == ETIMEDOUT) {
        goto return_null;
      }
    } else {
      if (pthread_cond_wait(&chan->cond, &chan->lock) == EINVAL) {
        goto return_null;
      }
    }
  }
  chanItem *item = chan->head;
  // this might happen if we got triggered by destroying the queue
  if (!item) goto return_null;

  chan->head = item->next;
  // empty queue...
  if (!chan->head) chan->tail = NULL;
  chan->size--;
  pthread_mutex_unlock(&chan->lock);
  // discard the item (TODO: recycle items)
  void *ret = item->ptr;
  free(item);
  return ret;

return_null:
  pthread_mutex_unlock(&chan->lock);
  return NULL;
}

void MRChannel_Close(MRChannel *chan) {
  pthread_mutex_lock(&chan->lock);
  chan->open = 0;
  pthread_mutex_unlock(&chan->lock);
}

void *MRChannel_Pop(MRChannel *chan) {
  return MRChannel_PopWait(chan, 0);
}
