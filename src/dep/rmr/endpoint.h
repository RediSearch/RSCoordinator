#ifndef __MR_ENDPOINT_H__
#define __MR_ENDPOINT_H__

/* A single endpoint in the cluster */
typedef struct MREndpoint {
  char *host;
  int port;
  char *unixSock;
} MREndpoint;

/* Parse a TCP address into an endpoint, in the format of host:port */
int MREndpoint_Parse(const char *addr, MREndpoint *ep);

/* Copy the endpoint's internal strings so freeing it will not hurt another copy of it */
void MREndpoint_Copy(MREndpoint *dst, const MREndpoint *src);

void MREndpoint_Free(MREndpoint *ep);

#endif