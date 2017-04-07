#include "parser_ctx.h"
#include "../dep/rmr/cluster.h"
#include "../dep/rmr/node.h"
#include "../dep/rmr/endpoint.h"

void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh) {

  int found = -1;
  for (int i = 0; i < t->numShards; i++) {
    if (sh->startSlot == t->shards[i].startSlot && sh->endSlot == t->shards[i].endSlot) {
      found = i;
      break;
    }
  }

  if (found >= 0) {
    MRClusterShard_AddNode(&t->shards[found], &sh->node);
  } else {
    MRClusterShard sh = MR_NewClusterShard(sh->startSlot, sh->endSlot, 2);
    MRClusterShard_AddNode(&sh, &sh->node);
    MRClusterTopology_AddShard(&t, &sh);
  }
}