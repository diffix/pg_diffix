#ifndef PG_OPENDIFFIX_EXECUTOR_H
#define PG_OPENDIFFIX_EXECUTOR_H

#include "postgres.h"
#include "executor/executor.h"

extern void pg_opendiffix_ExecutorStart(QueryDesc *queryDesc, int eflags);

extern void pg_opendiffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once);

extern void pg_opendiffix_ExecutorFinish(QueryDesc *queryDesc);

extern void pg_opendiffix_ExecutorEnd(QueryDesc *queryDesc);

#endif /* PG_OPENDIFFIX_EXECUTOR_H */
