#ifndef __MR_ENDPOINT_H__
#define __MR_ENDPOINT_H__

/* A single endpoint in the cluster */
typedef struct MREndpoint {
  char *host;
  int port;
  char *unixSock;
} MREndpoint;

int MREndpoint_Parse(const char *addr, MREndpoint *ep);

void MREndpoint_Free(MREndpoint *ep);

#endif