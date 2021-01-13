#ifndef PG_OPENDIFFIX_HOOKS_H
#define PG_OPENDIFFIX_HOOKS_H

/* Post parse analyze hook */
#include "parser/analyze.h"

/* Planner hook */
#include "optimizer/planner.h"

/* Executor hooks */
#include "executor/executor.h"

extern post_parse_analyze_hook_type prev_post_parse_analyze_hook;
extern planner_hook_type prev_planner_hook;
extern ExecutorStart_hook_type prev_ExecutorStart_hook;
extern ExecutorRun_hook_type prev_ExecutorRun_hook;
extern ExecutorFinish_hook_type prev_ExecutorFinish_hook;
extern ExecutorEnd_hook_type prev_ExecutorEnd_hook;

extern void pg_opendiffix_post_parse_analyze(ParseState *pstate, Query *query);

extern PlannedStmt *pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);

extern void pg_opendiffix_ExecutorStart(QueryDesc *queryDesc, int eflags);

extern void pg_opendiffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once);

extern void pg_opendiffix_ExecutorFinish(QueryDesc *queryDesc);

extern void pg_opendiffix_ExecutorEnd(QueryDesc *queryDesc);

#endif /* PG_OPENDIFFIX_HOOKS_H */
