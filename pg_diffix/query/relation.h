#ifndef PG_DIFFIX_RELATION_H
#define PG_DIFFIX_RELATION_H

#include "access/attnum.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

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
 * Data for a personal relation.
 */
typedef struct PersonalRelation
{
  Oid namespace_oid; /* Namespace OID */
  Oid oid;           /* Relation OID */
  List *aid_columns; /* AID columns in relation (of type AidColumn) */
} PersonalRelation;

extern List *gather_personal_relations(Query *query);

static inline bool involves_personal_relations(Query *query)
{
  List *personal_relations = gather_personal_relations(query);
  bool result = personal_relations != NIL;
  list_free(personal_relations);
  return result;
}

#endif /* PG_DIFFIX_RELATION_H */
