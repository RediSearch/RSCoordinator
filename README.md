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

$ mkdir build

$ python configure.py

$ cd buidl

$ make

```

This creates two files called `module-oss.so` and `module-enterprise.so` in /build, and from here on, you can run it inside redis oss cluster.

## Running RSCoordinator

To load the module just add the loadmodule parameter:

```
loadmodule /path/to/oss-module.so
```

The module automatically discovers the Redis cluster topology and distributes the search commands accordingly.

# Commands

See http://redisearch.io/Commands/

