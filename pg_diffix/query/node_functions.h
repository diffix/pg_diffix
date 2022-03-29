#ifndef PG_DIFFIX_NODE_FUNCTIONS_H
#define PG_DIFFIX_NODE_FUNCTIONS_H

extern bool walk_plan(Plan *plan, bool (*walker)(), void *context);

extern Plan *mutate_plan(Plan *plan, Plan *(*mutator)(), void *context);

#endif /* PG_DIFFIX_NODE_FUNCTIONS_H */
