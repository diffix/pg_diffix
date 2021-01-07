#include "pg_opendiffix/executor.h"
#include "pg_opendiffix/utils.h"

void pg_opendiffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  LOG_DEBUG("pg_opendiffix_ExecutorStart");
  standard_ExecutorStart(queryDesc, eflags);
}

void pg_opendiffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  LOG_DEBUG("pg_opendiffix_ExecutorRun");
  standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

void pg_opendiffix_ExecutorFinish(QueryDesc *queryDesc)
{
  LOG_DEBUG("pg_opendiffix_ExecutorFinish");
  standard_ExecutorFinish(queryDesc);
}

void pg_opendiffix_ExecutorEnd(QueryDesc *queryDesc)
{
  LOG_DEBUG("pg_opendiffix_ExecutorEnd");
  standard_ExecutorEnd(queryDesc);
}
