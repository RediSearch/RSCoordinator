#ifndef __RMR_REPLY_H__
#define __RMR_REPLY_H__

#include <stdlib.h>
#include "redismodule.h"


#define MR_REPLY_STRING 1
#define MR_REPLY_ARRAY 2
#define MR_REPLY_INTEGER 3
#define MR_REPLY_NIL 4
#define MR_REPLY_STATUS 5
#define MR_REPLY_ERROR 6


#ifndef __RMR_REPLY_C__
typedef struct MRReply MRReply;

void MRReply_Free(MRReply *reply);
int MRReply_Type(MRReply *reply);
long long MRReply_Integer(MRReply *reply);
size_t MRReply_Length(MRReply *reply);
char *MRReply_String(MRReply *reply, size_t *len);

MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx);

int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);
int MR_ReplyWithMRReply(RedisModuleCtx *ctx, MRReply *rep);

#endif

#endif