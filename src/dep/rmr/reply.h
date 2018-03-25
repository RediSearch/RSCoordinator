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

/* Get the array element from an array at index idx */
MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx);

/* Get the array element from an array and detach it from the array */
MRReply *MRReply_StealArrayElement(MRReply *r, size_t idx);
void MRReply_Print(FILE *fp, MRReply *r);
int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);
int MR_ReplyWithMRReply(RedisModuleCtx *ctx, MRReply *rep);

#endif

#endif