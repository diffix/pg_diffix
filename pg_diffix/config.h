#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "access/attnum.h"

#define INITIAL_NOISE_SEED "diffix"
#define INITIAL_NOISE_SIGMA 1.0
#define INITIAL_NOISE_CUTOFF 5.0

#define INITIAL_LOW_COUNT_THRESHOLD_MIN 2
#define INITIAL_LOW_COUNT_THRESHOLD_MAX 5

#define INITIAL_OUTLIER_COUNT_MIN 1
#define INITIAL_OUTLIER_COUNT_MAX 2

#define INITIAL_TOP_COUNT_MIN 4
#define INITIAL_TOP_COUNT_MAX 6

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

typedef struct OidCache
{
  DefaultAggregateOids postgres; /* Default aggregates */
  DiffixAggregateOids aid_int4;  /* Aggregates for int4 AID */
  DiffixAggregateOids aid_text;  /* Aggregates for text AID */
} OidCache;

/*
 * Configuration for a single relation.
 */
typedef struct RelationConfig
{
  char *rel_namespace_name; /* Namespace name */
  Oid rel_namespace_oid;    /* Namespace OID */
  char *rel_name;           /* Relation name */
  Oid rel_oid;              /* Relation OID */
  char *aid_attname;        /* AID column name */
  AttrNumber aid_attnum;    /* AID column AttNumber */
} RelationConfig;

/*
 * Root configuration object.
 */
typedef struct DiffixConfig
{
  char *noise_seed;
  double noise_sigma;
  double noise_cutoff;

  int low_count_threshold_min;
  int low_count_threshold_max;

  int outlier_count_min;
  int outlier_count_max;

  int top_count_min;
  int top_count_max;

  List *relations; /* Registered tables (of RelationConfig) */

  /*
   * OIDs that we need to rewrite aggregates.
   * Not strictly configuration, but convenient to load along with config.
   */
  OidCache oids;
} DiffixConfig;

/*
 * Global instance of root configuration.
 */
extern DiffixConfig Config;

/*
 * Loads and caches configuration.
 */
extern void load_diffix_config(void);

/*
 * Frees memory associated with cached configuration.
 */
extern void free_diffix_config(void);

/*
 * Looks up relation config by OID.
 * Returns NULL if the relation is not configured.
 */
extern RelationConfig *get_relation_config(DiffixConfig *config, Oid rel_oid);

/*
 * Formats config to a palloc'd string.
 */
extern char *config_to_string(DiffixConfig *config);

#endif /* PG_DIFFIX_CONFIG_H */
