#ifndef PG_DIFFIX_OID_CACHE_H
#define PG_DIFFIX_OID_CACHE_H

#include "c.h"

/*
 * OIDs needed for rewriting aggregates.
 */
typedef struct Oids
{
  Oid count;               /* count(*) */
  Oid count_any;           /* count(any) */
  Oid lcf;                 /* lcf(aid) */
  Oid anon_count_distinct; /* anon_count_distinct(aid) */
  Oid anon_count;          /* anon_count(aid) */
  Oid anon_count_any;      /* anon_count(aid, any) */
  Oid generate_series;     /* generate_series(aid, any) */
  bool loaded;             /* Whether the OIDs have been loaded */
} Oids;

/*
 * Global instance of OID cache.
 */
extern Oids g_oid_cache;

/*
 * Populates OID cache. Does nothing if cache is already loaded.
 * We don't call this at activation time because the UDFs may not be registered yet.
 */
extern void load_oid_cache(void);

/*
 * Frees OID cache for a clean plugin reload.
 * At the moment this simply sets loaded to false.
 */
extern void free_oid_cache(void);

/*
 * Formats OIDs to a palloc'd string.
 */
extern char *oids_to_string(Oids *oids);

#endif /* PG_DIFFIX_OID_CACHE_H */
