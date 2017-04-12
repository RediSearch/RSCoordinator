#include "minunit.h"
#include "../src/parser/parse.h"
#include "../src/dep/rmr/cluster.h"

void testParser() {
  const char *q =
      "MYID 1 HASREPLICATION RANGES 2 SHARD 1 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:20293 UNIXADDR "
      "unix:/tmp/redis-1.sock MASTER SHARD 2 SLOTRANGE 0 2047 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.50:20293 SHARD 3 SLOTRANGE 2048 "
      "4095 ADDR 7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.7:27262 UNIXADDR "
      "unix:/tmp/redis-3.sock MASTER SHARD 4 SLOTRANGE 2048 4095 ADDR "
      "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j@10.0.1.50:27262";

  char *err = NULL;
  MRClusterTopology *topo = ParseQuery(q, strlen(q), &err);
  if (err != NULL) {
    mu_fail(err);
  }
  mu_check(topo != NULL);

  mu_check(topo->numShards == 2);
  mu_check(topo->numSlots == 4096);

  mu_check(topo->shards[0].numNodes == 2);
  mu_check(topo->shards[0].startSlot == 0);
  mu_check(topo->shards[0].endSlot == 2047);
  mu_check(topo->shards[1].startSlot == 2048);
  mu_check(topo->shards[1].endSlot == 4095);

  mu_check(!strcmp(topo->shards[0].nodes[0].id, "1"));
  mu_check(!strcmp(topo->shards[0].nodes[0].endpoint.host, "10.0.1.7"));
  mu_check(topo->shards[0].nodes[0].endpoint.port == 20293);

  mu_check(!strcmp(topo->shards[0].nodes[1].id, "2"));
  mu_check(!strcmp(topo->shards[0].nodes[1].endpoint.host, "10.0.1.50"));
  mu_check(topo->shards[0].nodes[1].endpoint.port == 20293);

  mu_check(!strcmp(topo->shards[1].nodes[0].id, "3"));
  mu_check(!strcmp(topo->shards[1].nodes[0].endpoint.host, "10.0.1.7"));
  mu_check(topo->shards[1].nodes[0].endpoint.port == 27262);

  mu_check(!strcmp(topo->shards[1].nodes[1].id, "4"));
  mu_check(!strcmp(topo->shards[1].nodes[1].endpoint.host, "10.0.1.50"));
  mu_check(topo->shards[1].nodes[0].endpoint.port == 27262);

  for (int i = 0; i < topo->numShards; i++) {
    MRClusterShard *sh = &topo->shards[i];
    // printf("Shard %d: %d...%d, %d nodes: \n", i, sh->startSlot, sh->endSlot, sh->numNodes);
    for (int n = 0; n < sh->numNodes; n++) {

      mu_check(!strcmp(topo->shards[i].nodes[n].endpoint.auth,
                       "7EM5XV8XoDoazyvOnMOxbphgClZPGju2lZvm4pvDl3WHvk4j"));

      // MRClusterNode *node = &sh->nodes[n];

      // printf("\t node %d: id %s, ep %s@%s:%d\n", n, node->id, node->endpoint.auth,
      //        node->endpoint.host, node->endpoint.port);
    }
  }

  MRClusterTopology_Free(topo);

  // check failure path
  err = NULL;
  q = "foo bar baz";
  topo = ParseQuery(q, strlen(q), &err);
  mu_check(topo == NULL);
  mu_check(err != NULL);
  printf("\n%s\n", err);
}
int main(int argc, char **argv) {
  MU_RUN_TEST(testParser);

  MU_REPORT();

  return minunit_status;
}
