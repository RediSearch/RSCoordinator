#ifndef __RMR_REPLY_H__
#define __RMR_REPLY_H__

#include <stdlib.h>
#include "redismodule.h"
#include "hiredis/hiredis.h"

#define MR_REPLY_STRING 1
#define MR_REPLY_ARRAY 2
#define MR_REPLY_INTEGER 3
#define MR_REPLY_NIL 4
#define MR_REPLY_STATUS 5
#define MR_REPLY_ERROR 6

extern const int MRReply_UseV2;
extern const int MRReply_UseBlockAlloc;
extern redisReplyAccessors MRReply__Accessors;

typedef struct MRReply MRReply;

static inline void MRReply_Free(MRReply *reply) {
  freeReplyObject(reply);
}

static inline int MRReply_Type(MRReply *reply) {
  return MRReply__Accessors.getType(reply);
}

static inline long long MRReply_Integer(MRReply *reply) {
  return MRReply__Accessors.getInteger(reply);
}

static inline size_t MRReply_Length(MRReply *reply) {
  return MRReply__Accessors.getArrayLength(reply);
}

static inline char *MRReply_String(MRReply *reply, size_t *len) {
  return MRReply__Accessors.getString(reply, len);
}


static inline MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx) {
  return MRReply__Accessors.getArrayElement(reply, idx);
}

void MRReply_Print(FILE *fp, MRReply *r);
int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);
int MR_ReplyWithMRReply(RedisModuleCtx *ctx, MRReply *rep);

#endif