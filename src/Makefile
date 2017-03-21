#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=$(shell pwd)
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -dylib -exported_symbol _RedisModule_OnLoad
endif

ifndef DEBUG
	DEBUG = 0
endif

DEBUGFLAGS = -g -ggdb -O2
ifeq ($(DEBUG), 1)
	DEBUGFLAGS = -g -ggdb -O0
endif


CFLAGS = -I"$(RM_INCLUDE_DIR)" -Wall $(DEBUGFLAGS) -fPIC -std=gnu99  
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OBJS = module.o fnv.o search_cluster.o dep/crc16.o dep/heap.o
LIBS = dep/rmr/librmr.a dep/rmr/hiredis/libhiredis.a
export CFLAGS
export DEBUGFLAGS



all: module.so 

module.so: rmr $(OBJS)
	$(LD) -o $@ $(OBJS) $(LIBS) $(SHOBJ_LDFLAGS) -lc -lpthread -luv 

rmr:
	$(MAKE) -C dep/rmr all
.PHONY: rmr

clean:
	rm -rf *.xo *.so *.o

