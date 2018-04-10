#ifndef HIREDIS_REPLY_H
#define HIREDIS_REPLY_H

#include <stdlib.h>
#include <stdint.h>
#include "read.h"

#ifdef __cplusplush
extern "C" {
#endif

/* Mask returning the reply base type */
#define REDIS_REPLY_MASK 0xF

/* Mask returning type attributes */
#define REDIS_FLAG_MASK (0xFFFF00)

/* If this flag is set, then the reply is a more compact v2 version */
#define REDIS_REPLY_FLAG_V2 0x80

/* If this flag is set, then the reply has an embedded string, or a tiny array
 */
#define REDIS_REPLY_FLAG_TINY 0x100

/* Reply points to static memory. Do not free */
#define REDIS_REPLY_FLAG_STATIC 0x200

#define REDIS_REPLY_FLAG_BLKALLOC 0x400

#define REDIS_REPLY_FLAG_ROOT 0x800
/**
 * Maximum length of string that can be embedded in the reply object
 * TODO: make arch dependent (7 on 32, 15 on 64).
 * We might even be able to use another 4 bytes from the type
 */
#define REDIS_TINY_STR_BUFBYTES 15

/**
 * Maximum length of array that can be embedded in the reply object.
 * TODO: Make arch dependent; (1 on 32, 2 on 64)
 */
#define REDIS_TINY_ARRAY_ELEMS 2

typedef struct redisReplyV2 {
    uint32_t type;
    /* Padding for x64 */
    uint32_t intpad;
    union {
        long long integer;
        struct {
            char *str;
            size_t len;
        } str;
        struct {
            char str[REDIS_TINY_STR_BUFBYTES];
            uint8_t len;
        } tinystr;
        struct {
            struct redisReplyV2 **element;
            size_t elements;
        } array;
        struct {
            struct redisReplyV2 *elems[2];
        } tinyarray;
    } value;
} redisReplyV2;

extern redisReplyObjectFunctions redisReplyV2Functions;
extern redisReplyAccessors redisReplyV2Accessors;

/* This is the reply object returned by redisCommand() */
typedef struct redisReplyLegacy {
    int type;          /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    size_t len;        /* Length of string */
    char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements;             /* number of elements, for REDIS_REPLY_ARRAY */
    struct redisReplyLegacy **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReplyLegacy;

extern redisReplyObjectFunctions redisReplyLegacyFunctions;
extern redisReplyAccessors redisReplyLegacyAccessors;

void redisReaderEnableBlockAllocator(redisReader *r);

#define redisReplyGetType(res) ((*(int *)(res)) & REDIS_REPLY_MASK)
#define redisReplyGetFlags(res) ((*(int *)(res)) & REDIS_FLAG_MASK)

#define redisReplyGetInt(res)                       \
    (redisReplyGetFlags(res) & REDIS_REPLY_FLAG_V2) \
        ? ((redisReplyV2 *)res)->integer            \
        : ((redisReply *)res)->integer

static inline const char *redisReplyGetString(void *resp, size_t *lenp) {
    int flags = redisReplyGetFlags(resp);
    if (flags & REDIS_REPLY_FLAG_V2) {
        redisReplyV2 *v2 = (redisReplyV2 *)resp;
        if (flags & REDIS_REPLY_FLAG_TINY) {
            if (lenp) {
                *lenp = v2->value.tinystr.len;
            }
            return v2->value.tinystr.str;
        } else {
            if (lenp) {
                *lenp = v2->value.str.len;
            }
            return v2->value.str.str;
        }
    } else {
        redisReplyLegacy *legacy = (redisReplyLegacy *)resp;
        if (lenp) {
            *lenp = legacy->len;
        }
        return legacy->str;
    }
}

/**
 * These functions are inlined and can be used instead of the accessor
 * variants, if this is one of the built-in types. Otherwise, you should
 * use function pointers.
 */
static inline size_t redisReplyGetArrayLength(void *resp) {
    int flags = redisReplyGetFlags(resp);
    if (flags & REDIS_REPLY_FLAG_V2) {
        redisReplyV2 *v2 = (redisReplyV2 *)resp;
        if (flags & REDIS_REPLY_FLAG_TINY) {
            if (!v2->value.tinyarray.elems[0]) {
                return 0;
            }
            if (!v2->value.tinyarray.elems[1]) {
                return 1;
            }
            return 2;
        } else {
            return v2->value.array.elements;
        }
    } else {
        return ((redisReplyLegacy *)resp)->elements;
    }
}

static inline void *redisReplyGetArrayElement(void *resp, size_t pos) {
    int flags = redisReplyGetFlags(resp);
    if (flags & REDIS_REPLY_FLAG_V2) {
        redisReplyV2 *v2 = (redisReplyV2 *)resp;
        if (pos < 2 && (flags & REDIS_REPLY_FLAG_TINY)) {
            return v2->value.tinyarray.elems[pos];
        } else {
            return v2->value.array.element[pos];
        }
    } else {
        return ((redisReplyLegacy *)resp)->element[pos];
    }
}


#ifdef __cplusplus
}
#endif
#endif
