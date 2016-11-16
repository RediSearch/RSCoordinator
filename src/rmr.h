
#ifndef __LIBRMR_H__
#define __LIBRMR_H__

#include "reply.h"
#include "cluster.h"

struct MRCtx;

typedef int (*MRReduceFunc)(struct MRCtx *ctx, int count, MRReply **replies);

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, int argc, const char **argv);

void MR_Init(MRNodeProvider np);

void *MRCtx_GetPrivdata(struct MRCtx *ctx);
void MRCtx_Free(struct MRCtx *ctx);

struct MRCtx *MR_CreateCtx(void *ctx);




#endif //__LIBRMR_H__