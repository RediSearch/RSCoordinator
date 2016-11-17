# LibRMR - RedisMapReduce

## What?

This is a **Proof of Concept** of a library, that allows redis modules to communicate with nodes in a cluster in an asynchronous way. 

It allows the same command to be fanned-out to all the nodes, or send a list of commands, each to its relevant shard or node. 

It loosely follows a map/reduce pattern, where each command executed is considered a "map" operation, and a "reducer" callback is responsible for merging the results. 
A reducer can reply to the client, or trigger another map/reduce step. 

## NOTE: 

> **This is only a POC, and it only supports dummy cluster configuration and sharding. It works, but not really usable - just an example of how the API would work.** 

---

## Why?

The idea is to be able to scale module logic across many nodes, where a merged result is sent to the client. 

For example, let's say we have a search engine running on `N` redis instances. We can index `1/N` of our documents in each engine to scale it if more data is added. 

Then, when searching, we need to distribute the query to all nodes, and reduce the top N results from all nodes to a single list. 
RMR allows us to do it easily and abstracts the details of networking and threading. (it uses libuv and hiredis under the hood).

## How?

When loading the module, you need to initialize the RMR engine, and inject it with:

1. A *NodeProvider* - an interface supplying the engine with a list of the cluster's node (and in the future slots and other state info).

2. A *ShardFunc* - a callback that, given a list of nodes and a command's arguments, tells the engine which node/shard the command should be mapped to.

After the engine is initialized, you can trigger MapReduce steps from any of the module's command handlers. Two sorts of operations are supported:

1. *Map* - give the engine a list of commands, and it will execute each on its appropriate shard, and call a reducer with the results.

2. *FanOut* - give the engine a single command, and it will execute it on ALL shards, and call a reducer with the results.

## Example:

This SUM example takes a list of keys from the command arguments, performs GET on each key's appropriate shard, and reduces the result to a single sum (if they are numeric):

1. Triggering the map operation in a command handler: 

```c
/* RMR.SUM key key ... */
int SumCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) return RedisModule_WrongArity(ctx);
  
  /* Create a list of commands to distribute */
  MRCommand cmds[argc-1];
  for (int i = 0; i < argc - 1; i++) {
    cmds[i] = MR_NewCommand(2, "GET", RedisModule_StringPtrLen(argv[i+1], NULL));
  }

  /* Create a new MapReduce context wrapping our redis context */
  MRCtx *mc = MR_CreateCtx(ctx)

  /* Trigger a Map operation for the commands, with a reducer callback */ 
  MR_Map(mc, sumReducer, cmds, argc-1);

  return REDISMODULE_OK;
}

```

2. Summing up the results in the reducer:

(Note: `MRReply` is an abstraction built on hiredis reply objects, that has some convenience functions)

```c
/* A reducer that sums up numeric replies from a request */
int sumReducer(struct MRCtx *mc, int count, MRReply **replies) {

  /* Get the redis context saved in the MapReduce context */
  RedisModuleCtx *ctx = MRCtx_GetPrivdata(mc);
  long long sum = 0;
  for (int i = 0; i < count; i++) {
    long long n = 0;
    
    /* a convenience function to extract an integer value from the reply if possible */
    if (MRReply_ToInteger(replies[i], &n)) {
      sum += n;
    }
  }

   return RedisModule_ReplyWithLongLong(ctx, sum);
}
```
