#ifndef PG_DIFFIX_OID_CACHE_H
#define PG_DIFFIX_OID_CACHE_H

/*
 * OIDs needed for anonymization.
 */
typedef struct Oids
{
  /* Aggregators */
  Oid count_star;           /* count(*) */
  Oid count_value;          /* count(any) */
  Oid sum_int2;             /* sum(smallint) */
  Oid sum_int4;             /* sum(integer) */
  Oid sum_int8;             /* sum(bigint) */
  Oid sum_numeric;          /* sum(numeric) */
  Oid sum_float4;           /* sum(real) */
  Oid sum_float8;           /* sum(double precision) */
  Oid avg_int2;             /* avg(smallint) */
  Oid avg_int4;             /* avg(integer) */
  Oid avg_int8;             /* avg(bigint) */
  Oid avg_numeric;          /* avg(numeric) */
  Oid avg_float4;           /* avg(real) */
  Oid avg_float8;           /* avg(double precision) */
  Oid count_histogram;      /* diffix.count_histogram(any) */
  Oid count_histogram_int8; /* diffix.count_histogram(any, bigint) */

  Oid count_star_noise;  /* diffix.count_noise(*) */
  Oid count_value_noise; /* diffix.count_noise(any) */
  Oid sum_noise;         /* diffix.sum_noise(any) */
  Oid avg_noise;         /* diffix.avg_noise(any) */

  Oid low_count;            /* diffix.low_count(aids...) */
  Oid anon_count_distinct;  /* diffix.anon_count_distinct(any, aids...) */
  Oid anon_count_star;      /* diffix.anon_count_star(aids...) */
  Oid anon_count_value;     /* diffix.anon_count_value(any, aids...) */
  Oid anon_sum;             /* diffix.anon_sum(any, aids...) */
  Oid anon_count_histogram; /* diffix.anon_count_histogram(integer, bigint, aids...) */

  Oid anon_count_distinct_noise; /* diffix.anon_count_distinct_noise(any, aids...) */
  Oid anon_count_star_noise;     /* diffix.anon_count_star_noise(aids...) */
  Oid anon_count_value_noise;    /* diffix.anon_count_value_noise(any, aids...) */
  Oid anon_sum_noise;            /* diffix.anon_sum_noise(any, aids...) */

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

  /* Internal functions */
  Oid internal_qual_wrapper; /* diffix.internal_qual_wrapper(boolean) */

  Oid op_int8eq;
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

extern bool is_sum_oid(Oid aggoid);
extern bool is_avg_oid(Oid aggoid);

#endif /* PG_DIFFIX_OID_CACHE_H */
