#include <aggregate/aggregate_plan.h>
#include <util/arr.h>
#include "dist_plan.h"

#define PROPVAL(p) (RS_StringValFmt("@%s", RSKEY(p)))
/* The prototype for reducer distributor functions.
 * These functions take a reducer, and if possible, split into two reducers - a remote one and a
 * local one.
 *
 * It's also possible to add post-steps after the reduce step. For example, average will add an
 * apply step of sum/count
 *
 */
typedef int (*reducerDistributionFunc)(AggregateGroupReduce *src, AggregateStep *local,
                                       AggregateStep *remote);
reducerDistributionFunc getDistributionFunc(const char *key);

/* Extract the needed LOAD properties from the source plan to add to the dist plan */
void AggregatePlan_ExtractImplicitLoad(AggregatePlan *src, AggregatePlan *dist) {
  // Add a load step for everything not in the distributed schema
  AggregateSchema as = AggregatePlan_GetSchema(src, NULL);
  AggregateSchema dis = AggregatePlan_GetSchema(dist, NULL);

  // make an array of all the fields in the src schema that are not already in the dist schema
  const char **arr = array_new(const char *, 4);
  for (int i = 0; i < array_len(as); i++) {
    if (as[i].kind == Property_Field && !AggregateSchema_Contains(dis, as[i].property)) {
      arr = array_append(arr, RSKEY(as[i].property));
    }
  }

  // Add "APPLY @x AS @x" for each such property
  AggregateStep *q = AggregateStep_FirstOf(dist->head, AggregateStep_Query);

  for (int i = 0; i < array_len(arr); i++) {

    AggregateStep *a = AggregatePlan_NewApplyStepFmt(strdup(arr[i]), NULL, "@%s", arr[i]);

    AggregateStep_AddAfter(q ? q : dist->head, a);
  }

  array_free(arr);
  array_free(as);
  array_free(dis);
}

static AggregateStep *distributeGroupStep(AggregatePlan *src, AggregatePlan *dist,
                                          AggregateStep *step, int *cont) {
  AggregateGroupStep *gr = &step->group;

  AggregateStep *remoteStep = AggregatePlan_NewStep(AggregateStep_Group);
  AggregateStep *localStep = AggregatePlan_NewStep(AggregateStep_Group);

  AggregateGroupStep *remote = &remoteStep->group;
  AggregateGroupStep *local = &localStep->group;
  remote->idx = step->group.idx;
  local->idx = step->group.idx;

  remote->properties = RSMultiKey_Copy(gr->properties, 1);
  local->properties = RSMultiKey_Copy(gr->properties, 1);

  size_t nr = AggregateGroupStep_NumReducers(gr);
  remote->reducers = array_new(AggregateGroupReduce, nr);
  local->reducers = array_new(AggregateGroupReduce, nr);
  int ok = 1;
  for (int i = 0; i < nr && ok; i++) {
    reducerDistributionFunc fn = getDistributionFunc(gr->reducers[i].reducer);
    if (fn) {
      ok = fn(&gr->reducers[i], localStep, remoteStep);
    } else {
      ok = 0;
    }
  }
  *cont = 0;

  // we didn't manage to distribute all reducers, we have to revert to classic "get all rows" mode
  if (!ok) {
    AggregateStep_Free(remoteStep);
    AggregateStep_Free(localStep);

    return NULL;
  }

  // Succcess! we can distribute the aggregation!
  AggregatePlan_AddStep(dist, remoteStep);
  AggregateStep *tmp = AggregateStep_Detach(step);
  AggregateStep_AddBefore(tmp, localStep);
  AggregateStep_Free(step);
  return localStep->next;
}

static AggregateStep *distributeLimitStep(AggregateStep *step, AggregatePlan *dist) {
  AggregateStep *l = AggregatePlan_NewStep(AggregateStep_Limit);
  l->limit = step->limit;
  AggregatePlan_AddStep(dist, l);
  return step->next;
}

AggregatePlan *AggregatePlan_MakeDistributed(AggregatePlan *src) {
  AggregatePlan *dist = malloc(sizeof(*dist));
  AggregateStep *current = src->head;
  AggregatePlan_Init(dist);
  dist->cursor.count = 1000;
  dist->hasCursor = 1;
  dist->verbatim = src->verbatim;
  dist->withSchema = 1;
  // zero the stuff we don't care about in src
  dist->index = src->index;
  src->index = NULL;

  int cont = 1;

  while (current && cont) {
    switch (current->type) {
      case AggregateStep_Query:
      case AggregateStep_Apply:
      case AggregateStep_Load:
      case AggregateStep_Filter:

        current = AggregatePlan_MoveStep(src, dist, current);
        break;
      case AggregateStep_Limit:
        current = distributeLimitStep(current, dist);
        break;
      case AggregateStep_Group:

        current = distributeGroupStep(src, dist, current, &cont);
        break;
      case AggregateStep_Sort:
        cont = 0;
        current = NULL;
      case AggregateStep_Distribute:
        break;
      case AggregateStep_Dummy:
        current = current->next;
        break;
    }
  }

  // If needed, add implicit APPLY foo AS foo to the dist plan
  AggregatePlan_ExtractImplicitLoad(src, dist);

  // If we can distribute the plan, add a marker for it in the source plan
  AggregateStep *ds = AggregatePlan_NewStep(AggregateStep_Distribute);
  ds->dist.plan = dist;
  AggregateStep_AddAfter(src->head, ds);

  return dist;
}

/* Distribute COUNT into remote count and local SUM */
static int distributeCount(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {
  AggregateGroupStep_AddReducer(&remote->group, "COUNT", RSKEY(src->alias), 0);

  AggregateGroupStep_AddReducer(&local->group, "SUM", RSKEY(src->alias), 1, PROPVAL(src->alias));

  return 1;
}

/* Generic function to distribute an aggregator with a single argument as itself. This is the most
 * common case */
static int distributeSingleArgSelf(AggregateGroupReduce *src, AggregateStep *local,
                                   AggregateStep *remote) {
  // MAX must have a single argument
  if (array_len(src->args) != 1) {
    return 0;
  }
  AggregateGroupStep_AddReducer(&remote->group, src->reducer, RSKEY(src->alias), 1, src->args[0]);

  AggregateGroupStep_AddReducer(&local->group, src->reducer, RSKEY(src->alias), 1,
                                PROPVAL(src->alias));

  return 1;
}

#define RANDOM_SAMPLE_SIZE 500
/* Distribute QUANTILE into remote RANDOM_SAMPLE and local QUANTILE */
static int distributeQuantile(AggregateGroupReduce *src, AggregateStep *local,
                              AggregateStep *remote) {
  if (array_len(src->args) != 2) {
    return 0;
  }
  AggregateGroupStep_AddReducer(&remote->group, "RANDOM_SAMPLE", RSKEY(src->alias), 2, src->args[0],
                                RS_NumVal(RANDOM_SAMPLE_SIZE));

  AggregateGroupStep_AddReducer(&local->group, "QUANTILE", RSKEY(src->alias), 3,
                                PROPVAL(src->alias), src->args[1], RS_NumVal(RANDOM_SAMPLE_SIZE));

  return 1;
}

/* Distribute QUANTILE into remote RANDOM_SAMPLE and local QUANTILE */
static int distributeStdDev(AggregateGroupReduce *src, AggregateStep *local,
                            AggregateStep *remote) {
  if (array_len(src->args) != 1) {
    return 0;
  }
  AggregateGroupStep_AddReducer(&remote->group, "RANDOM_SAMPLE", RSKEY(src->alias), 2, src->args[0],
                                RS_NumVal(RANDOM_SAMPLE_SIZE));

  AggregateGroupStep_AddReducer(&local->group, "STDDEV", RSKEY(src->alias), 1, PROPVAL(src->alias));

  return 1;
}

/* Distribute COUNT_DISTINCTISH into HLL and MERGE_HLL */
static int distributeCountDistinctish(AggregateGroupReduce *src, AggregateStep *local,
                                      AggregateStep *remote) {
  if (array_len(src->args) != 1) {
    return 0;
  }

  AggregateGroupStep_AddReducer(&remote->group, "HLL", RSKEY(src->alias), 1, src->args[0]);
  AggregateGroupStep_AddReducer(&local->group, "HLL_SUM", RSKEY(src->alias), 1,
                                PROPVAL(src->alias));
  return 1;
}

/* Distribute AVG into remote SUM and COUNT, local SUM and SUM + apply SUM/SUM */
static int distributeAvg(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {
  // AVG must have a single argument
  if (array_len(src->args) != 1) {
    return 0;
  }
  // Add count and sum remotely
  char *countAlias = AggregateGroupStep_AddReducer(&remote->group, "COUNT", NULL, 0);
  char *sumAlias = AggregateGroupStep_AddReducer(&remote->group, "SUM", NULL, 1, src->args[0]);

  AggregateGroupStep_AddReducer(&local->group, "SUM", sumAlias, 1, PROPVAL(sumAlias));
  AggregateGroupStep_AddReducer(&local->group, "SUM", countAlias, 1, PROPVAL(countAlias));

  // We need to divide the sum of sums and the sum of counts to get the overall average
  char *err;
  AggregateStep *as =
      AggregatePlan_NewApplyStepFmt(src->alias, &err, "(@%s/@%s)", sumAlias, countAlias);
  if (!as) {
    return 0;
  }
  AggregateStep_AddAfter(local, as);
  return 1;
}

// Registry of available distribution functions
static struct {
  const char *key;
  reducerDistributionFunc func;
} reducerDistributors_g[] = {
    {"COUNT", distributeCount},
    {"SUM", distributeSingleArgSelf},
    {"MAX", distributeSingleArgSelf},
    {"MIN", distributeSingleArgSelf},
    {"AVG", distributeAvg},
    {"TOLIST", distributeSingleArgSelf},
    {"STDDEV", distributeStdDev},
    {"COUNT_DISTINCTISH", distributeCountDistinctish},
    {"QUANTILE", distributeQuantile},

    {NULL, NULL}  // sentinel value

};

reducerDistributionFunc getDistributionFunc(const char *key) {
  for (int i = 0; reducerDistributors_g[i].key != NULL; i++) {
    if (!strcasecmp(key, reducerDistributors_g[i].key)) {
      return reducerDistributors_g[i].func;
    }
  }

  return NULL;
}
