#include "minunit.h"
#include "../src/cluster.h"

void testEndpoint() {

  MREndpoint ep;
  mu_assert_int_eq(REDIS_OK, MREndpoint_Parse("localhost:6379", &ep));

  mu_check(!strcmp(ep.host, "localhost"));
  mu_assert_int_eq(6379, ep.port);
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost:-1", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost:655350", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse("localhost:", &ep));
  MREndpoint_Free(&ep);
  mu_assert_int_eq(REDIS_ERR, MREndpoint_Parse(":-1", &ep));
  MREndpoint_Free(&ep);
}

void testShardingFunc() {

  MRCommand cmd = MR_NewCommand("foo", 1, "baz");
  uint shard = CRC16ShardFunc(&cmd, 4096);
  mu_assert_int_eq(shard, 717);
  MRCommand_Free(&cmd);
}

MRClusterShard *_MRCluster_FindShard(MRCluster *cl, uint slot);

void testCluster() {

  int N = 4;
  const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  MRTopologyProvider tp =
      NewStaticTopologyProvider(4096, N, hosts[0], hosts[1], hosts[2], hosts[3]);

  StaticTopologyProvider *stp = tp.ctx;
  mu_check(stp != NULL);
  mu_check(stp->numNodes == N);
  mu_check(stp->numSlots == 4096);

  MRCluster *cl = MR_NewCluster(tp, CRC16ShardFunc);
  mu_check(cl != NULL);
  mu_check(cl->sf == CRC16ShardFunc);
  //  mu_check(cl->tp == tp);
  mu_check(cl->topo.numShards == N);
  mu_check(cl->topo.numSlots == 4096);

  for (int i = 0; i < cl->topo.numShards; i++) {
    MRClusterShard *sh = &cl->topo.shards[i];
    mu_check(sh->numNodes == 1);
    mu_check(sh->startSlot == i * (4096 / N));
    mu_check(sh->endSlot == sh->startSlot + (4096 / N) - 1);
    mu_check(!strcmp(sh->nodes[0].id, hosts[i]));

    printf("%d..%d --> %s\n", sh->startSlot, sh->endSlot, sh->nodes[0].id);
  }

  MRCommand cmd = MR_NewCommand("FT.SEARCH", 3, "foob", "bar", "baz");
  uint slot = CRC16ShardFunc(&cmd, cl->topo.numSlots);
  mu_check(slot > 0);
  MRClusterShard *sh = _MRCluster_FindShard(cl, slot);
  mu_check(sh != NULL);
  mu_check(sh->numNodes == 1);
  mu_check(!strcmp(sh->nodes[0].id, hosts[3]));
  printf("%d..%d --> %s\n", sh->startSlot, sh->endSlot, sh->nodes[0].id);

  // MRClust_Free(cl);
}

int main(int argc, char **argv) {
  MU_RUN_TEST(testEndpoint);
  MU_RUN_TEST(testShardingFunc);
  MU_RUN_TEST(testCluster);
  MU_REPORT();

  return minunit_status;
}
