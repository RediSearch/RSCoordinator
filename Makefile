all: build

build:
	$(MAKE) -C ./src module.so
.PHONY: build

test: build
	$(MAKE) -C ./test
	$(MAKE) -C ./src/dep/rmr/test test
	$(MAKE) -C ./src/dep/RediSearch/src redisearch.so test
	$(MAKE) -C ./src module-oss.so
	$(MAKE) -C ./pytest 
	
clean:
	$(MAKE) -C ./src clean
.PHONY: clean

deepclean:
	$(MAKE) -C ./src deepclean

BRANCH=$(shell git branch | awk '/\*/{print $$2}')
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
