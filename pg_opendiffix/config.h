#ifndef PG_OPENDIFFIX_CONFIG_H
#define PG_OPENDIFFIX_CONFIG_H

#include "postgres.h"
#include "nodes/pg_list.h"

typedef struct RelationConfig
{
  char *rel_namespace_name; /* Namespace name */
  Oid rel_namespace_oid;    /* Namespace OID */
  char *rel_name;           /* Relation name */
  Oid rel_oid;              /* Relation OID */
  char *aid_attname;        /* AID column name */
  AttrNumber aid_attnum;    /* AID column AttNumber */
} RelationConfig;

typedef struct OpenDiffixConfig
{
  List *relations; /* Registered tables (of RelationConfig) */
} OpenDiffixConfig;

extern OpenDiffixConfig *get_opendiffix_config(void);

extern OpenDiffixConfig *load_opendiffix_config(void);

extern void free_opendiffix_config(void);

extern char *config_to_string(OpenDiffixConfig *config);

#endif /* PG_OPENDIFFIX_CONFIG_H */
