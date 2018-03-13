#ifndef RSC_REDUCERS_H
#define RSC_REDUCERS_H

#include "dep/rmr/rmr.h"

/* A reducer that just chains the replies from a map request */
int ChainReplyReducer(struct MRCtx *ctx, int count, MRReply **replies);

/* A reducer that just merges N arrays of strings by chaining them into one big array with no
 * duplicates */
int UniqueStringsReducer(struct MRCtx *mc, int count, MRReply **replies);

/* A reducer that just merges N arrays of the same length, selecting the first non NULL reply from
 * each */
int MergeArraysReducer(struct MRCtx *mc, int count, MRReply **replies);

/**
 * Reducer which just replies with the first reply. The count must also be
 * one.
 */
int SingleReplyReducer(struct MRCtx *mc, int count, MRReply **replies);

/** a reducer that expects "OK" reply for all replies, and
 * stops at the first error and returns it
 */
int AllOKReducer(struct MRCtx *mc, int count, MRReply **replies);


#endif