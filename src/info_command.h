#ifndef INFO_COMMAND_H
#define INFO_COMMAND_H

#include "redismodule.h"
#include "dep/rmr/rmr.h"
#include "dep/rmr/reply.h"

int InfoReplyReducer(struct MRCtx *mc, int count, MRReply **replies);

#endif