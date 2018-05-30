#ifndef RS_DIST_PLAN_H_
#define RS_DIST_PLAN_H_

#include <aggregate/aggregate_plan.h>

AggregatePlan *AggregatePlan_MakeDistributed(AggregatePlan *src);

/****************************************************
 *
 *  Interanl API for aggregate plan manipulation.
 *  Should not be needed outside the scope of distributing the plan, but if needed it can be moved
 *into aggregate_plan.h
 ****************************************************/
char *AggregateGroupStep_AddReducer(AggregateGroupStep *g, const char *func, char *alias, int argc,
                                    ...);
size_t AggregateGroupStep_NumReducers(AggregateGroupStep *g);
char *AggregatePlan_GetReducerAlias(AggregateGroupStep *g, const char *func, RSValue **argv,
                                    int argc);
AggregateStep *AggregatePlan_NewApplyStepFmt(const char *alias, char **err, const char *fmt, ...);
AggregateStep *AggregatePlan_NewApplyStep(const char *alias, const char *expr, char **err);
AggregateStep *AggregatePlan_NewStep(AggregateStepType t);

AggregateStep *AggregatePlan_NewStep(AggregateStepType t);
void AggregateStep_Free(AggregateStep *s);

/* Add a step at the end of the plan */
void AggregatePlan_AddStep(AggregatePlan *plan, AggregateStep *step);
/* Add a step after a step */
void AggregateStep_AddAfter(AggregateStep *step, AggregateStep *add);
/* Add a step before a step */
void AggregateStep_AddBefore(AggregateStep *step, AggregateStep *add);
/* Detach the step and return the previous next of it */
AggregateStep *AggregateStep_Detach(AggregateStep *step);
/* Get the first step after start of type t */
AggregateStep *AggregateStep_FirstOf(AggregateStep *start, AggregateStepType t);
void AggregatePlan_Init(AggregatePlan *plan);
AggregateStep *AggregatePlan_MoveStep(AggregatePlan *src, AggregatePlan *dist, AggregateStep *step);
/* Extract the needed LOAD properties from the source plan to add to the dist plan */
void AggregatePlan_ExtractImplicitLoad(AggregatePlan *src, AggregatePlan *dist);

#endif