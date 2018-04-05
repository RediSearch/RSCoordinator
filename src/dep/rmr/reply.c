#define __RMR_REPLY_C__
#include "reply.h"
#include "hiredis/hiredis.h"
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <redismodule.h>

typedef redisReply MRReply;

const int MRReply_UseV2 = 1;
const int MRReply_UseBlockAlloc = 1;

/* Determine the function accessors */
static redisReplyAccessors* getAcc() {
  if (MRReply_UseV2) {
    return &redisReplyV2Accessors;
  } else {
    return &redisReplyLegacyAccessors;
  }
}
#define acc_g getAcc()

void MRReply_Free(MRReply *reply) {
  freeReplyObject(reply);
}

int MRReply_Type(MRReply *reply) {
  return acc_g->getType(reply);
}

void MRReply_Print(FILE *fp, MRReply *r) {
  if (!r) {
    fprintf(fp, "NULL");
    return;
  }

  switch (acc_g->getType(r)) {
    case MR_REPLY_INTEGER:
      fprintf(fp, "INT(%lld)", acc_g->getInteger(r));
      break;
    case MR_REPLY_STRING:
    case MR_REPLY_STATUS:
      fprintf(fp, "STR(%s)", acc_g->getString(r, NULL));
      break;
    case MR_REPLY_ERROR:
      fprintf(fp, "ERR(%s)", acc_g->getString(r, NULL));
      break;
    case MR_REPLY_NIL:
      fprintf(fp, "(nil)");
      break;
    case MR_REPLY_ARRAY:
      fprintf(fp, "ARR(%zd):[ ", acc_g->getArrayLength(r));
      for (size_t i = 0; i < acc_g->getArrayLength(r); i++) {
        MRReply_Print(fp, acc_g->getArrayElement(r, i));
        fprintf(fp, ", ");
      }
      fprintf(fp, "]");
      break;
  }
}

long long MRReply_Integer(MRReply *reply) {
  return acc_g->getInteger(reply);
}

size_t MRReply_Length(MRReply *reply) {
  return acc_g->getArrayLength(reply);
}

char *MRReply_String(MRReply *reply, size_t *len) {
  return acc_g->getString(reply, len);
}

MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx) {
  return acc_g->getArrayElement(reply, idx);
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


int MRReply_ToInteger(MRReply *reply, long long *i) {

  if (reply == NULL) return 0;

  switch (acc_g->getType(reply)) {
    case MR_REPLY_INTEGER:
      *i = acc_g->getInteger(reply);
      return 1;
    case MR_REPLY_STRING:
    case MR_REPLY_STATUS: {
      size_t n;
      const char *s = acc_g->getString(reply, &n);
      return _parseInt(s, n, i);
    }
    default:
      return 0;
  }
}

int MRReply_ToDouble(MRReply *reply, double *d) {
  if (reply == NULL) return 0;

  switch (acc_g->getType(reply)) {
    case MR_REPLY_INTEGER:
      *d = (double)acc_g->getInteger(reply);
      return 1;

    case MR_REPLY_STRING:
    case MR_REPLY_STATUS: {
      size_t n;
      const char *s = acc_g->getString(reply, &n);
      return _parseFloat(s, n, d);
    }

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
