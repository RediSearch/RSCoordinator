
ROOT:=$(shell pwd)

#----------------------------------------------------------------------------------------------
 
ifeq ($(DEBUG),1)
CONFIG_ARGS += --enable-debug
endif

all: build

export BUILD_DIR ?= build

configure:
	mkdir -p $(BUILD_DIR)
	set -e; cd $(BUILD_DIR); $(ROOT)/configure.py -j8 $(CONFIG_ARGS)

ifeq ($(wildcard $(BUILD_DIR)/Makefile),)
build: configure
else
build:
endif
	$(MAKE) -C $(BUILD_DIR)

.PHONY: build

clean:
	$(MAKE) -C src clean

.PHONY: clean

deepclean:
	$(MAKE) -C src deepclean

#----------------------------------------------------------------------------------------------
 
test: build
	$(MAKE) -C test
	$(MAKE) -C src/dep/rmr/test test
	$(MAKE) -C src/dep/RediSearch/src REDIS_MODULE_PATH=src/dep/RediSearch/src/redisearch.so redisearch.so test
	$(MAKE) -C src module-oss.so

PYTHON ?= python2

_PYTEST_ARGS=\
	-t ./src/dep/RediSearch/src/pytest/$(TEST) \
	--env oss-cluster --env-reuse \
	 --clear-logs \
	--module $(abspath $(BUILD_DIR)/module-oss.so) --module-args "$(strip PARTITIONS AUTO $(MODULE_ARGS))" \
	$(PYTEST_ARGS)

ifeq ($(GDB),1)
_PYTEST_ARGS += -s --shards-count 1 --debugger gdb
else
_PYTEST_ARGS += --shards-count 3
ifneq ($(TEST),)
_PYTEST_ARGS += -s
endif
endif

pytest:
	$(PYTHON) -m RLTest $(_PYTEST_ARGS)

#----------------------------------------------------------------------------------------------

BRANCH:=$(shell git branch | awk '/\*/{print $$2}')

docker:
	docker build . -t rscoordinator

docker_test: docker
	docker run -it --rm -v ~/.s3cfg:/root/.s3cfg -v `pwd`:/workspace rscoordinator make deepclean build test

# Create a package from the current branch and upload it to s3
docker_package: docker
	docker run -e BRANCH=$(BRANCH) -it --rm -v ~/.s3cfg:/root/.s3cfg -v `pwd`:/workspace rscoordinator

# RELEASES ONLY: Create the "latest" package and a package for the current version, and upload them to s3
docker_release: docker
	docker run -it --rm -v ~/.s3cfg:/root/.s3cfg -v -v `pwd`:/workspace rscoordinator make deepclean all test package_release upload

#----------------------------------------------------------------------------------------------
# Benchmark utility

ifneq ($(REMOTE),)
BENCHMARK_ARGS = run-remote
else
BENCHMARK_ARGS = run-local
endif

BENCHMARK_ARGS += --module_path $(abspath $(BUILD_DIR)/module-oss.so) \
	--required-module search

ifneq ($(BENCHMARK),)
BENCHMARK_ARGS += --test $(BENCHMARK)
endif

bench benchmark: $(TARGET)
	cd ./tests/benchmarks ;\
	redisbench-admin $(BENCHMARK_ARGS)

.PHONY: bench benchmark
