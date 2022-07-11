#ifndef PG_DIFFIX_UTILS_H
#define PG_DIFFIX_UTILS_H

#include "nodes/pg_list.h"
#include "utils/datum.h"

/*-------------------------------------------------------------------------
 * General utils
 *-------------------------------------------------------------------------
 */

/* Calculates the length of an array. */
#define ARRAY_LENGTH(arr) ((sizeof(arr)) / sizeof(arr[0]))

/*-------------------------------------------------------------------------
 * Hash utils
 *-------------------------------------------------------------------------
 */

typedef uint64 hash_t;
typedef hash_t seed_t;

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

static inline hash_t hash_string(const char *string)
{
  return hash_bytes(string, strlen(string));
}

static inline hash_t hash_datum(Datum value, bool typbyval, int16 typlen)
{
  const void *data = NULL;
  size_t data_size = 0;
  if (typbyval)
  {
    data = &value;
    data_size = sizeof(Datum);
  }
  else
  {
    data = DatumGetPointer(value);
    data_size = datumGetSize(value, false, typlen);
  }

  return hash_bytes(data, data_size);
}

static inline List *hash_set_add(List *hash_set, hash_t hash)
{
  return list_append_unique_ptr(hash_set, (void *)hash);
}

extern seed_t hash_set_to_seed(const List *hash_set);

static inline List *hash_set_union(List *dst_set, const List *src_set)
{
  return list_concat_unique_ptr(dst_set, src_set);
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
 * Errors and logging
 *-------------------------------------------------------------------------
 */

#define NOTICE_LOG(...) ereport(NOTICE, (errmsg("[PG_DIFFIX] " __VA_ARGS__)))

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

#endif /* PG_DIFFIX_UTILS_H */
