#ifndef __MR_NODE_H__
#define __MR_NODE_H__
#include "endpoint.h"

typedef enum { MRNode_Master = 0x1, MRNode_Self = 0x2, MRNode_Coordinator = 0x4 } MRNodeFlags;

typedef struct {
  MREndpoint endpoint;
  const char *id;
  MRNodeFlags flags;
} MRClusterNode;

#endif