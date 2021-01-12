#include "pg_opendiffix/executor.h"
#include "pg_opendiffix/utils.h"

void pg_opendiffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  standard_ExecutorStart(queryDesc, eflags);
}

void pg_opendiffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

void pg_opendiffix_ExecutorFinish(QueryDesc *queryDesc)
{
  standard_ExecutorFinish(queryDesc);
}

void pg_opendiffix_ExecutorEnd(QueryDesc *queryDesc)
{
  standard_ExecutorEnd(queryDesc);
}
