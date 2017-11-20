FROM redislabsmodules/rmbuilder as builder

ENV S3_CONFIG "/root/.s3cfg"
WORKDIR /workspace

CMD make deepclean all test && make -e BRANCH=${BRANCH} package upload

