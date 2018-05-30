#ifndef RQ_H__
#define RQ_H__

#include <stdlib.h>

typedef void (*MRQueueCallback)(void *);

#ifndef RQ_C__
typedef struct MRWorkQueue MRWorkQueue;

MRWorkQueue *RQ_New(size_t cap, int maxPending);

void RQ_Done(MRWorkQueue *q);

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata);
#endif
#endif