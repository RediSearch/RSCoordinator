FROM redis:latest as builder

ENV LIBDIR /var/lib/redis/modules
ENV DEPS "python python-setuptools python-pip wget unzip build-essential autoconf libtool automake"
# Set up a build environment
RUN set -ex;\
    deps="$DEPS";\
    apt-get update; \
	apt-get install -y --no-install-recommends $deps;
RUN set -ex;\    
    pip2 install rmtest s3cmd ramp-packer; 


# # Package the runner
# FROM redis:latest
# ENV LIBDIR /var/lib/redis/modules
# WORKDIR /data
# RUN set -ex;\
#     mkdir -p "$LIBDIR";
# COPY --from=builder /src/redisearch.so  "$LIBDIR"
ENV S3_CONFIG "/root/.s3cfg"
WORKDIR /src

CMD make deepclean all && make upload
# CMD ["redis-server", "--loadmodule", "/var/lib/redis/modules/redisearch.so"]
