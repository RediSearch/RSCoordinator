#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=$(abspath ../../..)
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../src/dep/rmutil
endif

SRC_DIR?=$(abspath ../)

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
LIBS:=  ../librmr.a ../hiredis/libhiredis.a ../../triemap/libtriemap.a 

TEST_SOURCES = $(abspath $(wildcard test_*.c))
TEST_OBJECTS = $(patsubst %.c, %.o, $(TEST_SOURCES)  )
TEST_EXECUTABLES = $(patsubst %.c, %.run, $(TEST_SOURCES)  )
%.run: %.o
	$(CC) $(CFLAGS) -o $@ $^  $(CFLAGS) $(OBJS)  -L"$(RM_INCLUDE_DIR)" $(LIBS) -lc -lpthread -luv -lm

all: build test

build: $(TEST_OBJECTS) $(TEST_EXECUTABLES)

# Test all
test: build $(TEST_EXECUTABLES)
	set -e; \
	for t in test_*.run;\
	 do ./$$t;\
	done

memcheck: build $(TEST_EXECUTABLES)
	set -e; \
	for t in test_*.run;\
	 do valgrind --tool=memcheck --leak-check=full --error-exitcode=1 --show-possibly-lost=no ./$$t;\
	done

# Target for individual tests - make run:{test name}, e.g. "make run:query" will run test_query.run
run\:%: 
	$(MAKE) test_$*.run
	./test_$*.run

clean:
	-rm -f *.o
	-rm -f *.run
	-rm -f ext-example/*.o ext-example/*.so 

.PHONY: clean

rebuild: clean all
