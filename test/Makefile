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
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I"$(RM_INCLUDE_DIR)" -Wall -g -fPIC  -O1 -std=gnu99  
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OBJS= ../src/search_cluster.o ../src/command.o ../src/fnv.o

test_searchcluster:
	$(CC) -o ./$@ $@.c $(OBJS) -lc -lpthread -luv
	$(sh -c ./$@)

.PHONY: test_searchcluster

clean:
	rm -rf *.xo *.so *.o

