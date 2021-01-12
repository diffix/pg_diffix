#include "pg_opendiffix/executor.h"
#include "pg_opendiffix/utils.h"

void pg_opendiffix_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
  LOG_DEBUG("pg_opendiffix_ExecutorStart (Query ID: %lu)", queryDesc->plannedstmt->queryId);
  standard_ExecutorStart(queryDesc, eflags);
}

void pg_opendiffix_ExecutorRun(
    QueryDesc *queryDesc,
    ScanDirection direction,
    uint64 count,
    bool execute_once)
{
  LOG_DEBUG("pg_opendiffix_ExecutorRun (Query ID: %lu)", queryDesc->plannedstmt->queryId);
  standard_ExecutorRun(queryDesc, direction, count, execute_once);
}

void pg_opendiffix_ExecutorFinish(QueryDesc *queryDesc)
{
  LOG_DEBUG("pg_opendiffix_ExecutorFinish (Query ID: %lu)", queryDesc->plannedstmt->queryId);
  standard_ExecutorFinish(queryDesc);
}

void pg_opendiffix_ExecutorEnd(QueryDesc *queryDesc)
{
  LOG_DEBUG("pg_opendiffix_ExecutorEnd (Query ID: %lu)", queryDesc->plannedstmt->queryId);
  standard_ExecutorEnd(queryDesc);
}
