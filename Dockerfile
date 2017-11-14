FROM redis:latest as builder

ENV LIBDIR /var/lib/redis/modules
ENV DEPS "python python-setuptools python-pip wget unzip build-essential autoconf libtool automake"
# Set up a build environment
RUN set -ex;\
    deps="$DEPS";\
    apt-get update; \
	apt-get install -y --no-install-recommends $deps;
RUN set -ex;\    
    pip2 install rmtest s3cmd ramp-packer>=1.3.7; 

ENV S3_CONFIG "/root/.s3cfg"
WORKDIR /src

CMD make deepclean all && make -e BRANCH=${BRANCH} package upload

