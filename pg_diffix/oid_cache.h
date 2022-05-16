#ifndef PG_DIFFIX_OID_CACHE_H
#define PG_DIFFIX_OID_CACHE_H

/*
 * OIDs needed for anonymization.
 */
typedef struct Oids
{
  /* Aggregators */
  Oid count_star;  /* count(*) */
  Oid count_value; /* count(any) */

  Oid low_count;           /* diffix.low_count(aids...) */
  Oid anon_count_distinct; /* diffix.anon_count_distinct(any, aids...) */
  Oid anon_count_star;     /* diffix.anon_count_star(aids...) */
  Oid anon_count_value;    /* diffix.anon_count_value(any, aids...) */

  Oid anon_agg_state; /* diffix.AnonAggState */

  /* Bucket-specific aggregates */
  Oid is_suppress_bin; /* diffix.is_suppress_bin(*) */

  /* Scalars */
  Oid round_by_nn; /* diffix.round_by(value numeric, amount numeric) */
  Oid round_by_dd; /* diffix.round_by(value double precision, amount double precision) */
  Oid ceil_by_nn;  /* diffix.ceil_by(value numeric, amount numeric) */
  Oid ceil_by_dd;  /* diffix.ceil_by(value double precision, amount double precision) */
  Oid floor_by_nn; /* diffix.floor_by(value numeric, amount numeric) */
  Oid floor_by_dd; /* diffix.floor_by(value double precision, amount double precision) */
} Oids;

/*
 * Global instance of OID cache.
 */
extern Oids g_oid_cache;

/*
 * Populates OID cache. Does nothing if cache is already loaded.
 * We don't call this at activation time because the UDFs may not be registered yet.
 */
extern void oid_cache_init(void);

/*
 * Frees OID cache for a clean plugin reload.
 * At the moment this simply sets loaded to false.
 */
extern void oid_cache_cleanup(void);

/*
 * Formats OIDs to a palloc'd string.
 */
extern char *oids_to_string(Oids *oids);

#endif /* PG_DIFFIX_OID_CACHE_H */
