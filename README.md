[![Release](https://img.shields.io/github/v/release/RedisLabsModules/RSCoordinator.svg?sort=semver)](https://github.com/RedisLabsModules/RSCoordinator/releases)
[![CircleCI](https://circleci.com/gh/RedisLabsModules/RSCoordinator.svg?style=svg&circle-token=4efb4a933bf11a44c122d33d68cda6b8b4163e15)](https://circleci.com/gh/RedisLabsModules/RSCoordinator)
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Gitter](https://badges.gitter.im/RedisLabs/RediSearch.svg)](https://gitter.im/RedisLabs/RediSearch?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

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
> FT.CREATE myIdx SCHEMA foo TEXT 

# Adding a document
> FT.ADD myIdx doc1 1.0 FIELDS foo "hello world"

# Searching
> FT.SEARCH myIdx "hello world"
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

# Packaging new versions

We use a docker image for Linux builds, and use that to generate RAMP packages. 

## Package a version of the current branch

```sh
make docker_package
```

This will generate a RAMP package with the current branch and upload it to s3, i.e. `s3://redismodules/redisearch-enterprise/redisearch-enterprise.Linux-x86_64.master.zip`

## Package release versions


```sh
make docker_release
```

This will generate two RAMP packages:

1. A package with the current version as defined in `version.h`. e.g. `redisearch-enterprise.Linux-x86_64.0.92.0.zip`

2. The "latest" package which always points to the latest stable release, e.g. `redisearch-enterprise.Linux-x86_64.latest.zip`


# Commands

See http://redisearch.io/Commands/
