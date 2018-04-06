#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=$(abspath ../../)
endif

LIBUV_INCLUDE_DIR=$(abspath ../libuv/include)
export LIBUV_INCLUDE_DIR
# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

DEBUGFLAGS ?= -O0 -g -ggdb
CFLAGS ?= -I$(RM_INCLUDE_DIR) -I$(LIBUV_INCLUDE_DIR) -Wall $(DEBUGFLAGS) -fPIC -std=gnu99  -DREDISMODULE_EXPERIMENTAL_API
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')

SOURCEDIR=$(shell pwd -P)
CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c) $(wildcard $(SOURCEDIR)/redise_parser/*.c)
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES))

LIBHIREDIS=hiredis/libhiredis.a

all: hiredis triemap librmr.a

%.c: %.y

hiredis:
	$(MAKE) -C ./hiredis static
.PHONY: hiredis

triemap:
	$(MAKE) -C ../triemap all
.PHONY: triemap


librmr.a: hiredis triemap | $(CC_OBJECTS)
	ar rcs $@ $(CC_OBJECTS)

test: librmr.a
	$(MAKE) -C ./test
clean:
	rm -rf *.o *.a

distclean: clean
	$(MAKE) -C ./hiredis clean
	find ./ -name "*.[oa]" -print -exec rm -f {} \;
