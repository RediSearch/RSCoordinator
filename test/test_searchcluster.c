#include "minunit.h"
#include "../src/search_cluster.h"
#include "../src/dep/rmr/command.h"
#include "../src/crc16_tags.h"

const char *FNVTagFunc(const char *key, size_t len, size_t k);
// void testTagFunc() {

//   SearchCluster sc = NewSearchCluster(100, FNVTagFunc);
//   MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "idx", "foo");

//   const char *tag = FNVTagFunc("hello", strlen("hello"), sc.size);
//   printf("%s\n", tag);
// }

void testCommandMux() {

  SearchCluster sc = NewSearchCluster(100, NewSimplePartitioner(100, crc16_slot_table, 16384));
  MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "idx", "foo");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(&sc, &cmd);

  MRCommand mxcmd;
  // printf("Expected len: %d\n", cg.Len(cg.ctx));
  while (cg.Next(cg.ctx, &mxcmd)) {
    MRCommand_Print(&mxcmd);
    MRCommand_Free(&mxcmd);
  }
  cg.Free(cg.ctx);
  sc.part.Free(sc.part.ctx);
}

int main(int argc, char **argv) {

  // MU_RUN_TEST(testTagFunc);
  MU_RUN_TEST(testCommandMux);

  MU_REPORT();
  return minunit_status;
}