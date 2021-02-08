#ifndef PG_DIFFIX_OID_CACHE_H
#define PG_DIFFIX_OID_CACHE_H

#include "postgres.h"

/*
 * OIDs of default Postgres aggregates.
 */
typedef struct DefaultAggregateOids
{
  Oid count;     /* count(*) */
  Oid count_any; /* count(any) */
} DefaultAggregateOids;

/*
 * OIDs of Diffix aggregates for some particular AID type.
 */
typedef struct DiffixAggregateOids
{
  Oid diffix_count;     /* diffix_count(aid) */
  Oid diffix_count_any; /* diffix_count(aid, any) */
  Oid diffix_lcf;       /* diffix_lcf(aid) */
} DiffixAggregateOids;

/*
 * Root object containing OIDs needed for rewriting aggregates.
 */
typedef struct AggregateOids
{
  DefaultAggregateOids postgres; /* Default aggregates */
  DiffixAggregateOids aid_int4;  /* Aggregates for int4 AID */
  DiffixAggregateOids aid_text;  /* Aggregates for text AID */
  bool loaded;                   /* Whether the OIDs have been loaded */
} AggregateOids;

/*
 * Global instance of OID cache.
 */
extern AggregateOids OidCache;

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
extern char *oids_to_string(AggregateOids *oids);

#endif /* PG_DIFFIX_OID_CACHE_H */
