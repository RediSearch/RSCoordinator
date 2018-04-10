#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "hiredis.h"

#define TEST_IP "localhost"
#define TEST_PORT 6379
#define TEST_CMD "FT.AGGREGATE gh * LOAD 2 @type @date"
#define ITERATIONS 10

#define USE_V2 1
#define USE_BLOCK_ALLOCATOR 1

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    redisOptions options = {.type = REDIS_CONN_TCP,
                            .endpoint.tcp = {.ip = TEST_IP, .port = TEST_PORT}};
    redisContext *ctx;
    redisReader *reader;
    redisReplyAccessors *acc;

    if (USE_V2) {
        reader = options.reader = redisReaderCreateWithFunctions(&redisReplyV2Functions);
        acc = options.accessors = &redisReplyV2Accessors;
        if (USE_BLOCK_ALLOCATOR) {
            redisReaderEnableBlockAllocator(reader);
        }
    }
    ctx = redisConnectWithOptions(&options);

    assert(ctx);
    for (size_t ii = 0; ii < ITERATIONS; ++ii) {
        redisReply *resp = redisCommand(ctx, TEST_CMD);
        assert(resp);
        assert(REDIS_REPLY_GETTYPE(ctx, resp) == REDIS_REPLY_ARRAY);
        freeReplyObject(resp);
    }
    redisFree(ctx);
    return 0;
}