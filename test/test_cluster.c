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

void testCluster() {
}

int main(int argc, char **argv) {
  MU_RUN_TEST(testEndpoint);
  MU_RUN_TEST(testShardingFunc);
  MU_REPORT();

  return minunit_status;
}
