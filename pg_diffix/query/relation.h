#ifndef PG_DIFFIX_RELATION_H
#define PG_DIFFIX_RELATION_H

#include "c.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "access/attnum.h"

/*
 * Data for an Anonymization ID column.
 */
typedef struct AidColumn
{
  AttrNumber attnum; /* AID column AttNumber */
  Oid atttype;       /* AID column type OID */
  int32 typmod;      /* AID pg_attribute typmod value */
  Oid collid;        /* AID collation */
} AidColumn;

/*
 * Data for a sensitive relation.
 */
typedef struct SensitiveRelation
{
  Oid namespace_oid; /* Namespace OID */
  Oid oid;           /* Relation OID */
  List *aid_columns; /* AID columns in relation (of type AidColumn) */
} SensitiveRelation;

extern List *gather_sensitive_relations(Query *query);

#endif /* PG_DIFFIX_RELATION_H */
