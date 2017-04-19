#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../../..
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../src/dep/rmutil
endif

SRC_DIR = ../

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
CFLAGS = -I"$(RM_INCLUDE_DIR)" -I"$(SRC_DIR)" -Wall -g -fPIC -O1 -std=gnu99  


# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
LIBS=  ../hiredis/libhiredis.a ../librmr.a ../../triemap/libtriemap.a


test_parser:
	$(CC) -o ./$@ $@.c $(CFLAGS) $(OBJS) $(LIBS) -lc -lpthread -luv
	$(sh -c ./$@)

test_cluster:
	$(CC) -o ./$@ $@.c $(OBJS) $(LIBS) -lc -lpthread -luv
	$(sh -c ./$@)

.PHONY: test_parser
.PHONY: test_cluster


clean:
	rm -rf *.xo *.so *.o

