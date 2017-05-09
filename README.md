# RSCoordinator - Distributed RediSearch

RSCoordinator is an add-on module that enables scalable distributed search over [RediSearch](http://redisearch.io).

## How It Works

RSCoordinator runs alongside RediSearch, and distributes search commands across the cluster. 
It translates its own API, which is similar to RediSearch's API, into a set of RediSearch commands, sends those to the appropriate shards,
and merges the responses to a single one. 

Most of the API that it exposes is identical to RediSearch's with the sole distinction, that instead of the prefix `FT` for all RediSearch commands, its prefix is `DFT` (short for Distributed Full Text).

### Example Usage

```
# Creating an index
> DFT.CREATE myIdx SCHEMA foo TEXT 

# Adding a document
> DFT.ADD myIdx doc1 1.0 FIELDS foo "hello world"

# Searching
> DFT.SEARCH myIdx "hello world"
```

The syntax of all these commands is identical to that of the equivalent RediSearch commands.

## Building RSCoordinator

RSCoordinator has no dependencies, and only needs gcc/lldb, automake and libc. It includes libuv internally, and uses the provided internal library.

Building is simply done by running:

```sh

$ cd /path/to/RSCoordinato/src

$ make all

```

This creates a file called `module.so` in /src, and from here on, you can run it inside redis.

## Running RSCoordinator

The module relies on configuration options, passed to it on loading, with the following syntax as a command-line argument or inside redis.conf:

```
loadmodule /path/to/module.so PARTITIONS {num_partitions} TYPE {redis_oss|redislabs} [ENDPOINT {[password@]host:port}]
```

Usually we run RediSearch and RSCoordinator on **all redis instances in our cluster**, and it's imperative to load them both, i.e.:

```
# Load RediSearch
loadmodule /path/to/redisearch/module.so 

# Load RSCoordinator
loadmodule /path/to/rscoordinator/module.so PARTITIONS 5 TYPE redislabs
```

### Configuration Options:

- **PARTITIONS {num_partitions}**: The number of *logical* partitions the index will use. As a rule of thumb, this should be equal to the number of shards (master redis instances) in the cluster. It must be greater than 0.

- **TYPE {redis_oss|redislabs}**: The cluster type. Since the cluster architectures differ, the module has adaptive logic for each of them.

  **redis_oss**: Use this when running the cluster on an open source redis cluster. When using this option, you must also specify our ENDPOINT (see below), so we can read the cluster state from it.

  **redislabs**: Use this to run RSCoordinator on Redis Labs Enterprise Cluster. In this case, there is no need to specify the endpoint.

- **[ENDPOINT {[password@]host:port}]**: For redis OSS cluster only. We use this option to poll redis for the cluster topology. Soon to be deprecated!

## Installing RSCoordinator on Redis Enterprise Cluster

TBD.