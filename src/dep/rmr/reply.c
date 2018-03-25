#define __RMR_REPLY_C__
#include "reply.h"
#include "hiredis/hiredis.h"
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <redismodule.h>

typedef redisReply MRReply;

void MRReply_Free(MRReply *reply) {
  freeReplyObject(reply);
}

int MRReply_Type(MRReply *reply) {
  return reply->type;
}

long long MRReply_Integer(MRReply *reply) {
  return reply->integer;
}

size_t MRReply_Length(MRReply *reply) {
  return reply->elements;
}

char *MRReply_String(MRReply *reply, size_t *len) {
  if (len) {
    *len = reply->len;
  }

  return reply->str;
}

MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx) {
  return reply->element[idx];
}

int _parseInt(char *str, size_t len, long long *i) {
  errno = 0; /* To distinguish success/failure after call */
  char *endptr = str + len;
  long long int val = strtoll(str, &endptr, 10);

  if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) || (errno != 0 && val == 0)) {
    perror("strtol");
    return 0;
  }

  if (endptr == str) {
    //  fprintf(stderr, "No digits were found\n");
    return 0;
  }

  *i = val;
  return 1;
}

int _parseFloat(char *str, size_t len, double *d) {
  errno = 0; /* To distinguish success/failure after call */
  char *endptr = str + len;
  double val = strtod(str, &endptr);

  /* Check for various possible errors */
  if (errno != 0 || (endptr == str && val == 0)) {
    return 0;
  }
  *d = val;
  return 1;
}

/* Get the array element from an array and detach it from the array */
MRReply *MRReply_StealArrayElement(MRReply *r, size_t idx) {
  if (!r || r->type != MR_REPLY_ARRAY || idx >= r->elements) {
    return NULL;
  }

  MRReply *ret = r->element[idx];
  r->element[idx] = NULL;
  return ret;
}

MRReply *MRReply_Duplicate(redisReply *src) {
  MRReply *ret = malloc(sizeof(MRReply));
  *ret = *src;

  if (src->str) {
    src->str = NULL;
    src->len = 0;
  }
  if (src->element && src->elements) {
    src->element = NULL;
    src->elements = 0;
  }
  src->type = REDIS_REPLY_NIL;
  // memset(rep, 0, sizeof(*rep));
  return ret;
}

int MRReply_ToInteger(MRReply *reply, long long *i) {

  if (reply == NULL) return 0;

  switch (reply->type) {
    case MR_REPLY_INTEGER:
      *i = reply->integer;
      return 1;
    case MR_REPLY_STRING:
    case MR_REPLY_STATUS:
      return _parseInt(reply->str, reply->len, i);
    default:
      return 0;
  }
}

int MRReply_ToDouble(MRReply *reply, double *d) {
  if (reply == NULL) return 0;

  switch (reply->type) {
    case MR_REPLY_INTEGER:
      *d = (double)reply->integer;
      return 1;

    case MR_REPLY_STRING:
    case MR_REPLY_STATUS:
      return _parseFloat(reply->str, reply->len, d);

    default:
      return 0;
  }
}

int MR_ReplyWithMRReply(RedisModuleCtx *ctx, MRReply *rep) {
  if (rep == NULL) {
    return RedisModule_ReplyWithNull(ctx);
  }
  switch (MRReply_Type(rep)) {

    case MR_REPLY_STRING: {
      size_t len;
      char *str = MRReply_String(rep, &len);
      return RedisModule_ReplyWithStringBuffer(ctx, str, len);
    }

    case MR_REPLY_STATUS:
      return RedisModule_ReplyWithSimpleString(ctx, MRReply_String(rep, NULL));

    case MR_REPLY_ARRAY: {
      RedisModule_ReplyWithArray(ctx, MRReply_Length(rep));
      for (size_t i = 0; i < MRReply_Length(rep); i++) {
        MR_ReplyWithMRReply(ctx, MRReply_ArrayElement(rep, i));
      }
      break;
    }

    case MR_REPLY_INTEGER:
      return RedisModule_ReplyWithLongLong(ctx, MRReply_Integer(rep));

    case MR_REPLY_ERROR:
      return RedisModule_ReplyWithError(ctx, MRReply_String(rep, NULL));

    case MR_REPLY_NIL:
    default:
      return RedisModule_ReplyWithNull(ctx);
  }
  return REDISMODULE_ERR;
}
