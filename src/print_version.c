#ifdef PRINT_VERSION_TARGET
#include <stdio.h>
#include "version.h"

/* This is a utility that prints the current semantic version string, to be used in make files */

int main(int argc, char **argv) {
  printf("%d.%02d.%02d\n", RSCOORDINATOR_VERSION_MAJOR, RSCOORDINATOR_VERSION_MINOR,
         RSCOORDINATOR_VERSION_PATCH);
  return 0;
}
#endif