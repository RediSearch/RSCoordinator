#include "redise.h"
#include "dep/rmutil/util.h"

static int ParseToken(RedisModuleCtx *ctx,
            int optional_arg,
            char* token_string,
            RedisModuleString **argv,
            int argc,
            int start_offset,
            int end_offset, // use argc to search until the end
            int expected_token_position,
            const char *fmt, ...) {

  int token_offset = RMUtil_ArgExists(token_string, argv, argc, start_offset);

  if ((!token_offset) || (token_offset > end_offset)){
    if(optional_arg) {
      return REDISMODULE_OK;
    }
    RedisModule_Log(ctx, "error", "Parsing error. %s token is missing btween positions %d and %d.", token_string, start_offset, end_offset);
    return REDISMODULE_ERR;
  }

  if(token_offset != expected_token_position) {
    RedisModule_Log(ctx, "error",
    "Parsing error. %s token should be at position %d but is at %d.",
    token_string,
    expected_token_position,
    token_offset);
    return REDISMODULE_ERR;
  }

  va_list ap;
  va_start(ap, fmt);
  int rv = RMUtil_VParseArgs(argv, argc, token_offset + 1, fmt, ap);
  va_end(ap);

  return rv;
}

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                 int argc) {
  int res;
  const int num_slots = 4096;
  const int mandatory = 0;
  const int optional = 1;
  const size_t max_field_len = 100;

  RedisModule_Log(ctx, "notice", "Start parsing argc is %d", argc);

  char *my_id = NULL;
  MRClusterTopology *topo = NULL;

  int expected_token_pos = 1;
  //my_id = (char*)calloc(max_field_len, sizeof(char));
  res = ParseToken(ctx, mandatory, "MYID", argv, argc, 0, argc, expected_token_pos, "c", &my_id);
  if(res != REDISMODULE_OK || !my_id) {
    goto err;
  }

  expected_token_pos += 2;
  int has_replication_token_pos = RMUtil_ArgExists("HASREPLICATION", argv, argc, 0);
  if ((has_replication_token_pos != 0) && (has_replication_token_pos != expected_token_pos)) {
    goto err;
  }
  // should be a bool.
  int has_replication = has_replication_token_pos ? 1 : 0;

  if (has_replication) {
    expected_token_pos += 1;
  }
  long long int num_masters = 0;
  res = ParseToken(ctx, mandatory, "RANGES", argv, argc, 0, argc, expected_token_pos, "l", &num_masters);
  if(res != REDISMODULE_OK) {
    goto err;
  }

  topo = calloc(1, sizeof(MRClusterTopology));

  topo->numShards = num_masters;
  topo->numSlots = num_slots;
  topo->shards = calloc(num_masters, sizeof(MRClusterShard));

  int num_slots_per_shard = num_slots/num_masters;
  int num_nodes_per_shard = has_replication ? 2 : 1;

  expected_token_pos += 2;
  //int expected_token_pos = first_shard_expected_pos;

  for (int i = 0; i < (num_masters * num_nodes_per_shard); i++) {

    char *id = NULL;
    res = ParseToken(ctx, mandatory, "SHARD", argv, argc,
                     expected_token_pos, expected_token_pos, expected_token_pos, "c", &id);
    if(res != REDISMODULE_OK || !id) {
      goto err;
    }
    id = strdup(id);

    expected_token_pos += 2;

    long long int start_slot, end_slot;
    res = ParseToken(ctx, mandatory, "SLOTRANGE", argv, argc,
                     expected_token_pos, expected_token_pos, expected_token_pos, "ll", &start_slot, &end_slot);
    if(res != REDISMODULE_OK) {
      goto err;
    }
    expected_token_pos += 3;

    char *addr = NULL;/*(char*)calloc(max_field_len, sizeof(char));*/
    res = ParseToken(ctx, mandatory, "ADDR", argv, argc,
                     expected_token_pos, expected_token_pos, expected_token_pos, "c", &addr);
    if(res != REDISMODULE_OK || !addr) {
      goto err;
    }
    addr = strdup(addr);

    expected_token_pos += 2;

    char *unix_addr = NULL;/*(char*)calloc(max_field_len, sizeof(char));*/
    res = ParseToken(ctx, optional, "UNIXADDR", argv, argc,
                     expected_token_pos, expected_token_pos, expected_token_pos, "c", &unix_addr);
    if(res != REDISMODULE_OK) {
      goto err;
    }

    if (unix_addr) {
      unix_addr = strdup(unix_addr);
      expected_token_pos += 2;
    }

    int is_slave = 1;
    int master_token_pos = RMUtil_ArgExists("MASTER", argv, argc, expected_token_pos);
    if ((master_token_pos != 0) && (master_token_pos == expected_token_pos)) {
      // MASTER TOKEN is present for this shard
      is_slave = 0;
      expected_token_pos += 1;
    }

    int shard_index = start_slot/num_slots_per_shard;

    //check if it's the first time we parse this slot range.
    if (!topo->shards[shard_index].nodes) {
      topo->shards[shard_index].nodes =
                (MRClusterNode*)calloc(num_nodes_per_shard, sizeof(MRClusterNode));
      topo->shards[shard_index].startSlot = start_slot;
      topo->shards[shard_index].endSlot = end_slot;
      topo->shards[shard_index].numNodes = num_nodes_per_shard;
    }

    MRClusterNode node;
    node.id = id;
    //check for errors
    int status = MREndpoint_Parse(addr, &node.endpoint);
    node.endpoint.unixSock = unix_addr;

    node.flags = MRNode_Coordinator;
    if (!is_slave) {
      node.flags |= MRNode_Master;
    }
    //change to compare strings...
    if (strcmp(id,my_id) == 0) {
      node.flags |= MRNode_Self;
    }

    topo->shards[shard_index].nodes[is_slave] = node;
  }

  return topo;

err:
  RedisModule_Log(ctx, "error", "Error parsing cluster topology");
  if (topo) {
    MRClusterTopology_Free(topo);
  }
  return NULL;
}



