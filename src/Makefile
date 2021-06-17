#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
RM_INCLUDE_DIR:=$(shell pwd)
endif

ifndef S3_CONFIG
S3_CONFIG=~/.s3cfg
endif

SOURCEDIR:=$(shell pwd -P)

# Setting the DEBUG env variable to 1 will cause us to build with -O0
ifndef DEBUG
DEBUG = 0
endif

DEBUGFLAGS = -g -ggdb -O2
ifeq ($(DEBUG),1)
DEBUGFLAGS = -g -ggdb -O0
endif

LIBUV_DIR = $(RM_INCLUDE_DIR)/dep/libuv
LIBUV_LIBDIR = $(LIBUV_DIR)/.libs
LIBUV_INCLUDE_DIR = $(LIBUV_DIR)/include
REDISEARCH_DIR = $(SOURCEDIR)/dep/RediSearch/src

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

CFLAGS := \
	-I"$(RM_INCLUDE_DIR)" \
	-I"$(LIBUV_INCLUDE_DIR)" \
	-I"$(REDISEARCH_DIR)" \
	-Wall \
	-Wno-unused-function \
	$(DEBUGFLAGS) \
	-fPIC \
	-std=gnu99 \
	-D_GNU_SOURCE \
	-DREDIS_MODULE_TARGET \
	-DREDISMODULE_EXPERIMENTAL_API

CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
SHOBJ_LDFLAGS ?= -shared -Bsymbolic -Bsymbolic-functions
else
CFLAGS += -mmacosx-version-min=10.6
SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
SHOBJ_LDFLAGS ?= -dylib -exported_symbol _RedisModule_OnLoad -macosx_version_min 10.6
endif

export CFLAGS
export DEBUGFLAGS
export LIBUV_INCLUDE_DIR

CC_SOURCES = $(wildcard $(SOURCEDIR)/*.c) $(wildcard $(SOURCEDIR)/dep/*.c)
CC_OBJECTS = $(patsubst $(SOURCEDIR)/%.c, $(SOURCEDIR)/%.o, $(CC_SOURCES))

LIBS = \
	dep/rmr/librmr.a \
	dep/rmr/hiredis/libhiredis.a \
	dep/triemap/libtriemap.a \
	libuv.a \
	dep/RediSearch/src/libredisearch.a \
	dep/RediSearch/src/trie/libtrie.a \
	dep/RediSearch/src/dep/libnu/libnu.a \
	dep/RediSearch/src/dep/friso/libfriso.a \
	dep/RediSearch/src/dep/miniz/libminiz.a \
	dep/RediSearch/src/dep/snowball/libstemmer.o \
	dep/rmutil/librmutil.a

all: module.so

libuv.a:  $(LIBUV_DIR)/Makefile
	$(MAKE) -C $(LIBUV_DIR) && cp "$(LIBUV_LIBDIR)/libuv.a" ./

$(LIBUV_DIR)/Makefile: $(LIBUV_DIR)/configure
	cd $(LIBUV_DIR) && ./configure

$(LIBUV_DIR)/configure:
	cd $(LIBUV_DIR) && sh autogen.sh

libredisearch.a:
	make -j 8 -C dep/RediSearch/src libredisearch.a

INFOSTUB_E = build-info/info-enterprise.o
INFOSTUB_OSS = build-info/info-oss.o

module.so: rmr rmutil libuv.a libredisearch.a $(LIBS) $(CC_OBJECTS) $(INFOSTUB_E)
	$(LD) -o $@ $(CC_OBJECTS) $(INFOSTUB_E) $(LIBS) $(SHOBJ_LDFLAGS) -lc -lpthread -ldl -lm

module-oss.so: rmr rmutil libuv.a libredisearch.a $(LIBS) $(CC_OBJECTS) $(INFOSTUB_OSS)
	$(LD) -o $@ $(CC_OBJECTS) $(INFOSTUB_OSS) $(LIBS) $(SHOBJ_LDFLAGS) -lc -lpthread -ldl -lm

# OSS is a special build for internal testing on OSS clusters.
# Unless the module is built with "make oss" it cannot run inside OSS cluster
oss: module-oss.so


rmr:
	$(MAKE) -j 4 -C dep/rmr hiredis all
.PHONY: rmr

rmutil:
	$(MAKE) -j 4 -C dep/rmutil all
.PHONY: rmutil


# Format the version printing utility
print_version: version.h print_version.c
	@$(CC) -o $@ -DPRINT_VERSION_TARGET $@.c
.PHONY: print_version

## Package the module into a RAMP package on master builds and version tag builds
CWD:=$(shell pwd)
PACKAGE_NAME=redisearch-enterprise
ifndef BRANCH
BRANCH:=$(shell git branch | awk '/\*/{print $$2}')
endif
RAMP_COMMAND=ramp-packer
BUILD_DIR:=$(shell mktemp -d)

# Build a RELEASE package(s): the version number plus a "latest" package
package_release: module.so print_version
	$(RAMP_COMMAND) -m ramp.yml --output "$(BUILD_DIR)/$(PACKAGE_NAME).{os}-{architecture}.`./print_version`.zip" "$(CWD)/module.so";\
	$(RAMP_COMMAND) -m ramp.yml --output "$(BUILD_DIR)/$(PACKAGE_NAME).{os}-{architecture}.latest.zip" "$(CWD)/module.so";\
	$(RAMP_COMMAND) -m ramp-lightrce.yml --output "$(BUILD_DIR)/$(PACKAGE_NAME).{os}-{architecture}-lightrce.`./print_version`.zip" "$(CWD)/module.so";\
	$(RAMP_COMMAND) -m ramp-lightrce.yml --output "$(BUILD_DIR)/$(PACKAGE_NAME).{os}-{architecture}-lightrce.latest.zip" "$(CWD)/module.so";\

# Build a package for the current branch
package: module.so
	@# On normal build - package the branch latest
	$(RAMP_COMMAND) -m ramp.yml --output "$(BUILD_DIR)/$(PACKAGE_NAME).{os}-{architecture}.$(BRANCH).zip" "$(CWD)/module.so";\
	$(RAMP_COMMAND) -m ramp-lightrce.yml --output "$(BUILD_DIR)/$(PACKAGE_NAME).{os}-{architecture}-lightrce.$(BRANCH).zip" "$(CWD)/module.so";\

# Upload package files from the build dir to s3
upload:
	s3cmd -c $(S3_CONFIG) put $(BUILD_DIR)/*.zip --acl-public s3://redismodules/$(PACKAGE_NAME)/

clean:
	rm -rvf *.xo *.so *.o *.a

deepclean: clean
	$(MAKE) -C dep/RediSearch distclean || true
	$(MAKE) -C dep/libuv distclean || true
	$(MAKE) -C dep/rmr distclean || true
	$(MAKE) -C dep/triemap distclean || true
	rm -f print_version
	find ./ -name "*.[oa]" -exec rm {} \; -print
	find ./ -name "*.l[oa]" -exec rm {} \; -print
	find ./ -name "*.so" -exec rm {} \; -print
