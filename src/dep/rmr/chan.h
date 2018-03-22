#ifndef MR_CHAN_H_
#define MR_CHAN_H_
#include <stdlib.h>

#ifndef MR_CHAN_C_
typedef struct MRChannel MRChannel;
#endif

extern void *MRCHANNEL_CLOSED;

MRChannel *MR_NewChannel(size_t max);
int MRChannel_Push(MRChannel *chan, void *ptr);
void *MRChannel_Pop(MRChannel *chan);
void *MRChannel_PopWait(MRChannel *chan, int waitMS);
void MRChannel_Close(MRChannel *chan);
size_t MRChannel_Size(MRChannel *chan);
size_t MRChannel_MaxSize(MRChannel *chan);
void MRChannel_Free(MRChannel *chan);
#endif