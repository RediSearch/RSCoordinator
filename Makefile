all: bootstrap build

bootstrap:
	git submodule init && git submodule update

build:
	$(MAKE) -C ./src all
.PHONY: build

clean:
	$(MAKE) -C ./src clean
.PHONY: clean

docker_package:
	docker build . -t rscoordinator
	docker run -it --rm -v ~/.s3cfg:/root/.s3cfg -v ./src:/src rscoordinator