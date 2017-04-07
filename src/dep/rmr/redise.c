#include "redise.h"
#include "dep/rmutil/util.h"

static int ParseToken(RedisModuleCtx *ctx,
            int optional_arg,
            char* token_string,
            RedisModuleString **argv,
            int argc,
            int offset,
            int expected_token_position,
            const char *fmt, ...) {

  int token_offset = RMUtil_ArgExists(token_string, argv, argc, offset);

  if (!token_offset){
    if(optional_arg) {
      return REDISMODULE_OK;
    }
    RedisModule_Log(ctx, "error", "Parsing error. %s token is missing.", token_string);
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
  int rv = RMUtil_ParseArgs(argv, argc, token_offset, fmt, ap);
  va_end(ap);

  return rv;
}

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                 int argc) {
  int res;
  const int num_slots = 4096;
  const int mandatory = 0;
  const int optinal = 1;
  const size_t len = 100;

  char *my_id = NULL;
  MRClusterTopology *topo = NULL;

  const int MYID_expected_pos = 1;
  my_id = (char*)calloc(len, sizeof(char));
  res = ParseToken(ctx, mandatory, "MYID", argv, argc, 0, MYID_expected_pos, "c", my_id);
  if(res != REDISMODULE_OK) {
    goto err;
  }

  const int MASTERS_expected_pos = 3;
  long long int num_masters = 0;
  res = ParseToken(ctx, mandatory, "MASTERS", argv, argc, 0, MASTERS_expected_pos, "l", &num_masters);
  if(res != REDISMODULE_OK) {
    goto err;
  }

  const int REPLICATION_expected_pos = 5;
  int replication_token_pos = RMUtil_ArgExists("REPLICATION", argv, argc, 0);
  if ((replication_token_pos != 0) && (replication_token_pos != REPLICATION_expected_pos)) {
    goto err;
  }
  // should be a bool.
  int replication = replication_token_pos ? 1 : 0;

  topo = calloc(1, sizeof(MRClusterTopology));

  topo->numShards = num_masters;
  topo->numSlots = num_slots;
  topo->shards = calloc(num_masters, sizeof(MRClusterShard));

  int num_slots_per_shard = num_slots/num_masters;
  int num_nodes_per_shard = replication ? 2 : 1;

  const int first_shard_expected_pos = 6;
  int expected_token_pos = first_shard_expected_pos;

  for (int i = 0; i < (num_masters * num_nodes_per_shard); i++) {

    char *id = (char*)calloc(len, sizeof(char));
    res = ParseToken(ctx, mandatory, "SHARD", argv, argc,
                     expected_token_pos, expected_token_pos, "c", id);
    if(res != REDISMODULE_OK) {
      goto err;
    }
    expected_token_pos += 2;

    long long int start_slot, end_slot;
    res = ParseToken(ctx, mandatory, "SLOTRANGE", argv, argc,
                     expected_token_pos, expected_token_pos, "ll", &start_slot, &end_slot);
    if(res != REDISMODULE_OK) {
      goto err;
    }
    expected_token_pos += 3;

    char *addr = (char*)calloc(len, sizeof(char));
    res = ParseToken(ctx, mandatory, "ADDR", argv, argc,
                     expected_token_pos, expected_token_pos, "c", addr);
    if(res != REDISMODULE_OK) {
      goto err;
    }
    expected_token_pos += 2;

    char *unix_addr = (char*)calloc(len, sizeof(char));
    res = ParseToken(ctx, optinal, "UNIXADDR", argv, argc,
                     expected_token_pos, expected_token_pos, "c", unix_addr);
    if(res != REDISMODULE_OK) {
      goto err;
    }
    if (unix_addr[0] == 0) {
      //the optional UNIXADDR field is not present
      free(unix_addr);
    } else {
      expected_token_pos += 2;
    }

    int master_token_pos = RMUtil_ArgExists("MASTER", argv, argc, expected_token_pos);
    if ((replication_token_pos != 0) && (replication_token_pos != expected_token_pos)) {
      goto err;
    }
    // should be a bool.
    int is_slave = master_token_pos ? 0 : 1;
    expected_token_pos += !is_slave;

    int shard_index = start_slot/num_slots_per_shard;

    //check if it's the first time we parse this slot range.
    if (!topo->shards[shard_index].nodes) {
      topo->shards[shard_index].nodes =
                (MRClusterNode*)calloc(num_nodes_per_shard, sizeof(MRClusterNode));
      topo->shards[shard_index].startSlot = start_slot;
      topo->shards[shard_index].startSlot = end_slot;
      topo->shards[shard_index].numNodes = num_slots_per_shard;
    }

    MRClusterNode node;
    node.id = id;
    //check for errors
    int status = MREndpoint_Parse(addr, &node.endpoint);
    node.endpoint.unixSock = unix_addr;

    node.flags = MRNode_Coordinator;
    if (!is_slave) {
      node.flags &= MRNode_Master;
    }
    //change to compare strings...
    if (id == my_id) {
      node.flags &= MRNode_Self;
    }

    topo->shards[shard_index].nodes[is_slave] = node;
  }

  return topo;

err:
  RedisModule_Log(ctx, "error", "Error parsing cluster topology");
  if(my_id) {
    free(my_id);
  }
  MRClusterTopology_Free(topo);
  return RedisModule_ReplyWithError(ctx, "Parsing Error");
}



