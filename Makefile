all: bootstrap build

bootstrap:
	git submodule init && git submodule update

build:
	$(MAKE) -C ./src all
.PHONY: build

test: build
	$(MAKE) -C ./test
	$(MAKE) -C ./src/dep/rmr/test
	$(MAKE) -C ./src/dep/RediSearch/src redisearch.so test
	
clean:
	$(MAKE) -C ./src clean
.PHONY: clean

BRANCH=$(shell git branch | awk '/\*/{print $$2}')
docker:
	docker build . -t rscoordinator

# Create a package from the current branch and upload it to s3
docker_package: docker
	docker run -e BRANCH=$(BRANCH) -it --rm -v ~/.s3cfg:/root/.s3cfg -v `pwd`/src:/src rscoordinator

# RELEASES ONLY: Create the "latest" package and a package for the current version, and upload them to s3
docker_release: docker
	docker run -it --rm -v ~/.s3cfg:/root/.s3cfg -v `pwd`/src:/src rscoordinator make deepclean all package_release upload