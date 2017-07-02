all: bootstrap build

bootstrap:
	git submodule init && git submodule update

build:
	$(MAKE) -C ./src all
.PHONY: build

clean:
	$(MAKE) -C ./src clean
.PHONY: clean
