#include <string.h>
#include <stdlib.h>
#include "endpoint.h"
#include "hiredis/hiredis.h"

int MREndpoint_Parse(const char *addr, MREndpoint *ep) {

  ep->host = NULL;
  ep->unixSock = NULL;
  char *colon = strchr(addr, ':');
  if (!colon || colon == addr) {
    return REDIS_ERR;
  }

  ep->host = strndup(addr, colon - addr);
  ep->port = atoi(colon + 1);

  if (ep->port <= 0 || ep->port > 0xFFFF) {
    return REDIS_ERR;
  }
  return REDIS_OK;
}

void MREndpoint_Copy(MREndpoint *dst, const MREndpoint *src) {
  *dst = *src;
  if (src->host) {
    dst->host = strdup(src->host);
  }
  if (src->unixSock) {
    dst->host = strdup(src->host);
  }
}

void MREndpoint_Free(MREndpoint *ep) {
  if (ep->host) {
    free(ep->host);
    ep->host = NULL;
  }
  if (ep->unixSock) {
    free(ep->unixSock);
    ep->unixSock = NULL;
  }
}
