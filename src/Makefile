#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../rmutil
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -exported_symbol _RedisModule_OnLoad
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -g -ggdb -fPIC  -O0 -std=gnu99  
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OBJS= module.o rmr.o reply.o cluster.o command.o fnv.o search_cluster.o dep/crc16.o

all: module.so 

module.so: $(OBJS)
	$(LD) -o $@ $(OBJS) $(SHOBJ_LDFLAGS) $(LIBS) -lc -lpthread  -luv -L./hiredis -lhiredis 

clean:
	rm -rf *.xo *.so *.o

