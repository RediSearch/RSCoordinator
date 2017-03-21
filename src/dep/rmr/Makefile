#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

DEBUGFLAGS ?= -O0 -g -ggdb
CFLAGS ?= -I$(RM_INCLUDE_DIR) -Wall $(DEBUGFLAGS) -fPIC -std=gnu99  

CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OBJS=rmr.o reply.o cluster.o command.o
LIBHIREDIS=hiredis/libhiredis.a

hiredis:
	$(MAKE) -C ./hiredis static

.PHONY: hiredis

all: librmr.a

librmr.a: hiredis $(OBJS)
	ar rcs $@ $(OBJS)

clean:
	rm -rf *.o *.a

