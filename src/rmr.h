
#ifndef __LIBRMR_H__
#define __LIBRMR_H__

#include "hiredis/hiredis.h"

struct MRCtx;
struct MREndpoint; 
struct MRCluster;


typedef int (*MRReduceFunc)(struct MRCtx *ctx, int count, redisReply **replies);

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, int argc, const char **argv);

void MR_Init(struct MRCluster *cl);

void *MRCtx_GetPrivdata(struct MRCtx *ctx);


struct MRCtx *MR_CreateCtx(struct MRCluster *cl, void *ctx);
void MRCluster_ConnectAll(struct MRCluster *cl); 
struct MRCluster *MR_NewCluster(int numNodes, struct MREndpoint *nodes); 


#endif //__LIBRMR_H__