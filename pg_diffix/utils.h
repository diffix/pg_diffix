#ifndef PG_DIFFIX_UTILS_H
#define PG_DIFFIX_UTILS_H

#include "c.h"
#include "access/htup.h"
#include "access/htup_details.h" /* Convenience import for `heap_getattr`. */
#include "access/tupdesc.h"
#include "utils/elog.h"

/*-------------------------------------------------------------------------
 * Table utils
 *-------------------------------------------------------------------------
 */

/*
 * Retrieves relation OID from the namespace/relname pair.
 */
extern Oid get_rel_oid(const char *rel_ns_name, const char *rel_name);

typedef void *(*MapTupleFunc)(HeapTuple heap_tuple, TupleDesc tuple_desc);

/*
 * Opens a table and maps tuples to custom data using `map_tuple`.
 * NULLs returned by `map_tuple` are ignored.
 */
extern List *scan_table(Oid rel_oid, MapTupleFunc map_tuple);

static inline List *scan_table_by_name(
    const char *rel_ns_name,
    const char *rel_name,
    MapTupleFunc map_tuple)
{
  Oid rel_oid = get_rel_oid(rel_ns_name, rel_name);
  return scan_table(rel_oid, map_tuple);
}

/*-------------------------------------------------------------------------
 * Errors and logging
 *-------------------------------------------------------------------------
 */

#define FAILWITH(...) ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

#define FAILWITH_LOCATION(cursorpos, ...) \
  ereport(ERROR, (errmsg("[PG_DIFFIX] " __VA_ARGS__), errposition((cursorpos) + 1)))

#define FAILWITH_CODE(code, ...) ereport(ERROR, (errcode(code), errmsg("[PG_DIFFIX] " __VA_ARGS__)))

#define DEBUG /* TODO: define externally */

#ifdef DEBUG

#define DEBUG_LOG(...) ereport(LOG, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))
#define DEBUG_DUMP_NODE(label, node) ereport(LOG, (errmsg("[PG_DIFFIX] %s %s", label, nodeToString(node))))

#else

#define DEBUG_LOG(...)
#define DEBUG_DUMP_NODE(label, node)

#endif

#endif /* PG_DIFFIX_UTILS_H */
