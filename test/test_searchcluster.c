#include "minunit.h"
#include "../src/search_cluster.h"
#include "../src/command.h"
const char *FNVTagFunc(const char *key, size_t len, size_t k);
// void testTagFunc() {

//   SearchCluster sc = NewSearchCluster(100, FNVTagFunc);
//   MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "idx", "foo");

//   const char *tag = FNVTagFunc("hello", strlen("hello"), sc.size);
//   printf("%s\n", tag);
// }

void testCommandMux() {
  SearchCluster sc = NewSearchCluster(100, NewSimplePartitioner(100));
  MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "idx", "foo");

  SCCommandMuxIterator cm = SearchCluster_MultiplexCommand(&sc, &cmd, 1);

  MRCommand mxcmd;
  while (SCCommandMuxIterator_Next(&cm, &mxcmd)) {
    MRCommand_Print(&mxcmd);
    MRCommand_Free(&mxcmd);
  }

  sc.part.Free(sc.part.ctx);
}

int main(int argc, char **argv) {

  // MU_RUN_TEST(testTagFunc);
  MU_RUN_TEST(testCommandMux);

  MU_REPORT();
  return minunit_status;
}