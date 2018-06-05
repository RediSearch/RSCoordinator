ADD_LIBRARY(hiredis STATIC
    async.c
    block_alloc.c
    dict.c
    hiredis.c
    net.c
    read.c
    reply.c
    sds.c)