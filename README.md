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

RSCoordinator has no dependencies, and only needs **gcc/lldb, automake, libtool and libc** to build it. It includes libuv internally, and uses the provided internal library.

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

There is an automated [fabric](http://fabfile.org) based script to install and configure a search cluster using RSCoordinator and RediSearch, under the `fab` folder in this repo. To install and configure a cluster, follow the following steps:

### 0. Install and configure RLEC cluster

The steps for that are not detailed in this scope. We assume you have a cluster ready with a version that supports modules. 

### 1. Install fabric

(Assuming you have pip installed, if not install it first):

```sh
$ sudo pip install fabric
```

### 2. Configure S3 credentials

You must provide an s3 configuration file that has access to the `s3://redismodules` bucket, where the module builds are stored.
Copy that file as `s3cfg` to `fab/res` inside the RSCoordinator project:

```sh
$ cp /path/to/.s3cfg /path/to/RSCoordinator/fab/res/s3cfg
```

### 3. Download the modules to the cluster master machine

```sh
$ cd RSCoordinator/fab
# Change the ip of the RLEC cluster master accordingly
$ export RL_MASTER=1.2.3.4
$ fab download_modules
```

### 4. Create the database with the modules

You need to provide:
1. The desired database name
2. The number of shards

```sh
$ cd RSCoordinator/fab
# Change the ip of the RLEC cluster master accordingly
$ export RL_MASTER=1.2.3.4
# Change the database name and number of shards accordingly
$ fab create_database:my_db,10
```

# Commands

# 