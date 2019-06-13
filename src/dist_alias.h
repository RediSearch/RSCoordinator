#ifndef DIST_ALIAS_H
#define DIST_ALIAS_H
#ifdef __cplusplus
extern "C" {
#endif

// Call from module init to set up proper hooks
void ClusterAlias_Init(void);

// Call to get the real name of the alias
const char *ClusterAlias_Get(const char *alias);

#ifdef __cplusplus
}
#endif
#endif