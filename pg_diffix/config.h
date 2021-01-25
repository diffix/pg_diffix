#ifndef PG_DIFFIX_CONFIG_H
#define PG_DIFFIX_CONFIG_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "access/attnum.h"

#define INITIAL_NOISE_SEED "diffix"
#define INITIAL_NOISE_SIGMA 1.0

#define INITIAL_LOW_COUNT_THRESHOLD_MIN 2
#define INITIAL_LOW_COUNT_THRESHOLD_MAX 5

#define INITIAL_OUTLIER_COUNT_MIN 1
#define INITIAL_OUTLIER_COUNT_MAX 2

#define INITIAL_TOP_COUNT_MIN 4
#define INITIAL_TOP_COUNT_MAX 6

typedef struct RelationConfig
{
  char *rel_namespace_name; /* Namespace name */
  Oid rel_namespace_oid;    /* Namespace OID */
  char *rel_name;           /* Relation name */
  Oid rel_oid;              /* Relation OID */
  char *aid_attname;        /* AID column name */
  AttrNumber aid_attnum;    /* AID column AttNumber */
} RelationConfig;

typedef struct DiffixConfig
{
  char *noise_seed;
  double noise_sigma;

  int low_count_threshold_min;
  int low_count_threshold_max;

  int outlier_count_min;
  int outlier_count_max;

  int top_count_min;
  int top_count_max;

  List *relations; /* Registered tables (of RelationConfig) */
} DiffixConfig;

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
