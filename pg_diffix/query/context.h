#ifndef PG_DIFFIX_CONTEXT_H
#define PG_DIFFIX_CONTEXT_H

#include "c.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "access/attnum.h"

/*
 * Data for a sensitive relation.
 */
typedef struct DiffixRelation
{
  char *rel_namespace_name; /* Namespace name */
  Oid rel_namespace_oid;    /* Namespace OID */
  char *rel_name;           /* Relation name */
  Oid rel_oid;              /* Relation OID */
  Index rel_index;          /* Relation index in query rtable */
  char *aid_attname;        /* AID column name */
  AttrNumber aid_attnum;    /* AID column AttNumber */
  Oid aid_atttype;          /* AID column type OID */
  int32 aid_typmod;         /* AID pg_attribute typmod value */
  Oid aid_collid;           /* AID collation */
} DiffixRelation;

/*
 * Data needed for validating and rewriting queries.
 */
typedef struct QueryContext
{
  Query *query;    /* Currently executing query */
  List *relations; /* Sensitive relations in query (of DiffixRelation) */
} QueryContext;

extern QueryContext build_query_context(Query *query);

#endif /* PG_DIFFIX_CONTEXT_H */
