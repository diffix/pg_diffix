#ifndef PG_DIFFIX_UTILS_H
#define PG_DIFFIX_UTILS_H

#include "fmgr.h"
#include "access/htup.h"
#include "access/htup_details.h" /* Convenience import for `heap_getattr`. */
#include "access/tupdesc.h"

/*-------------------------------------------------------------------------
 * General utils
 *-------------------------------------------------------------------------
 */

typedef uint64 hash_t;

static inline hash_t hash_bytes(const void *bytes, size_t size)
{
  /* Implementation of FNV-1a hash algorithm: http://www.isthe.com/chongo/tech/comp/fnv/index.html */
  const uint64 FNV_PRIME = 1099511628211UL;
  const uint64 OFFSET_BASIS = 14695981039346656037UL;

  hash_t hash = OFFSET_BASIS;
  for (size_t i = 0; i < size; i++)
  {
    uint8 octet = ((const uint8 *)bytes)[i];
    hash = (hash ^ octet) * FNV_PRIME;
  }

  return hash;
}

/*-------------------------------------------------------------------------
 * Compatibility shims
 *-------------------------------------------------------------------------
 */
#if PG_MAJORVERSION_NUM < 13
#error "This module requires PostgreSQL version 13 or higher!"
#elif PG_MAJORVERSION_NUM >= 14
#define getObjectTypeDescription(object) getObjectTypeDescription(object, false)
#endif

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

#ifdef DEBUG

#define DEBUG_LOG(...) ereport(LOG,                                \
                               errmsg("[PG_DIFFIX] " __VA_ARGS__), \
                               errhidestmt(true))

#define DEBUG_DUMP_NODE(label, node)                      \
  do                                                      \
  {                                                       \
    char *node_str = nodeToString(node);                  \
    ereport(LOG,                                          \
            errmsg("[PG_DIFFIX] %s %s", label, node_str), \
            errhidestmt(true));                           \
    pfree(node_str);                                      \
  } while (0)

#else

#define DEBUG_LOG(...)
#define DEBUG_DUMP_NODE(label, node)

#endif

/*-------------------------------------------------------------------------
 * Aggregation utils
 *-------------------------------------------------------------------------
 */

/*
 * Switches to the aggregation memory context.
 */
extern MemoryContext switch_to_aggregation_context(PG_FUNCTION_ARGS);

/*
 * Checks if the current aggregation group has no grouping columns.
 * Also detects the case with multiple grouping sets where one is global (empty grouping set).
 */
extern bool is_global_aggregation(PG_FUNCTION_ARGS);

#endif /* PG_DIFFIX_UTILS_H */
