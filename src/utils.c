#include "postgres.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "nodes/execnodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_diffix/utils.h"

Oid get_rel_oid(const char *rel_ns_name, const char *rel_name)
{
  Oid rel_ns = get_namespace_oid(rel_ns_name, false);
  Oid rel_oid = get_relname_relid(rel_name, rel_ns);
  return rel_oid;
}

List *scan_table(Oid rel_oid, MapTupleFunc map_tuple)
{
  Assert(map_tuple != NULL);

  if (!OidIsValid(rel_oid))
    return NIL;

  PushActiveSnapshot(GetTransactionSnapshot());

  Relation table = table_open(rel_oid, AccessShareLock);
  SysScanDesc scan = systable_beginscan(table, InvalidOid, false, NULL, 0, NULL);

  TupleDesc tuple_desc = RelationGetDescr(table);
  HeapTuple heap_tuple = systable_getnext(scan);
  List *result = NIL;
  while (HeapTupleIsValid(heap_tuple))
  {
    void *data = map_tuple(heap_tuple, tuple_desc);
    if (data != NULL)
      result = lappend(result, data);

    heap_tuple = systable_getnext(scan);
  }

  systable_endscan(scan);
  table_close(table, AccessShareLock);

  PopActiveSnapshot();

  return result;
}

MemoryContext switch_to_aggregation_context(PG_FUNCTION_ARGS)
{
  MemoryContext agg_context;
  if (AggCheckCallContext(fcinfo, &agg_context) != AGG_CONTEXT_AGGREGATE)
    FAILWITH("Aggregate called in non-aggregate context");

  return MemoryContextSwitchTo(agg_context);
}

bool is_global_aggregation(PG_FUNCTION_ARGS)
{
  if (fcinfo->context && IsA(fcinfo->context, AggState))
  {
    AggState *agg_state = (AggState *)fcinfo->context;
    Agg *agg_plan = (Agg *)agg_state->ss.ps.plan;
    return agg_plan->numCols == 0;
  }

  return false;
}
