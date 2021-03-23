#ifndef PG_DIFFIX_CONTEXT_H
#define PG_DIFFIX_CONTEXT_H

#include "c.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "access/attnum.h"

/*
 * Data for an anonymization ID.
 */
typedef struct
{
  char *attname;     /* AID column name */
  AttrNumber attnum; /* AID column AttNumber */
  Oid atttype;       /* AID column type OID */
  int32 typmod;      /* AID pg_attribute typmod value */
  Oid collid;        /* AID collation */
} AnonymizationID;

/*
 * Data for a sensitive relation.
 */
typedef struct
{
  char *namespace_name; /* Namespace name */
  Oid namespace_oid;    /* Namespace OID */
  char *name;           /* Relation name */
  Oid oid;              /* Relation OID */
  Index index;          /* Relation index in query rtable */
  List *aids;           /* AIDs in relation (of type AnonymizationID) */
} SensitiveRelation;

/*
 * Data needed for validating and rewriting queries.
 */
typedef struct QueryContext
{
  Query *query;    /* Currently executing query */
  List *relations; /* Sensitive relations in query (of type SensitiveRelation) */
} QueryContext;

extern QueryContext build_query_context(Query *query);

#endif /* PG_DIFFIX_CONTEXT_H */
