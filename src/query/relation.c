#include "postgres.h"
#include "catalog/namespace.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/table.h"
#include "utils/rel.h"

#include "pg_diffix/auth.h"
#include "pg_diffix/utils.h"
#include "pg_diffix/query/relation.h"

static SensitiveRelation *create_sensitive_relation(Oid rel_oid, Oid namespace_oid)
{
  SensitiveRelation *sensitive_rel = palloc(sizeof(SensitiveRelation));
  sensitive_rel->namespace_oid = namespace_oid;
  sensitive_rel->oid = rel_oid;
  sensitive_rel->aid_columns = NIL;

  Relation rel = table_open(rel_oid, AccessShareLock);
  TupleDesc rel_desc = RelationGetDescr(rel);

  for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
  {
    Form_pg_attribute att = TupleDescAttr(rel_desc, i);

    if (is_aid_column(rel_oid, att->attnum))
    {
      AidColumn *aid_col = palloc(sizeof(AidColumn));
      aid_col->attnum = att->attnum;
      aid_col->atttype = att->atttypid;
      aid_col->typmod = att->atttypmod;
      aid_col->collid = att->attcollation;

      sensitive_rel->aid_columns = lappend(sensitive_rel->aid_columns, aid_col);
    }
  }

  table_close(rel, AccessShareLock);

  return sensitive_rel;
}

static bool has_relation(List *relations, Oid rel_oid)
{
  ListCell *lc;
  foreach (lc, relations)
  {
    SensitiveRelation *rel = (SensitiveRelation *)lfirst(lc);
    if (rel->oid == rel_oid)
      return true;
  }

  return false;
}

static bool gather_sensitive_relations_walker(Node *node, List **relations)
{
  if (node == NULL)
    return false;

  if (IsA(node, RangeTblEntry))
  {
    RangeTblEntry *rte = (RangeTblEntry *)node;
    if (!OidIsValid(rte->relid) || has_relation(*relations, rte->relid))
      return false;

    Oid namespace_oid = get_rel_namespace(rte->relid);
    if (is_sensitive_relation(rte->relid, namespace_oid))
    {
      SensitiveRelation *rel_data = create_sensitive_relation(rte->relid, namespace_oid);
      *relations = lappend(*relations, rel_data);
    }
  }
  else if (IsA(node, Query))
  {
    Query *query = (Query *)node;
    range_table_walker(
        query->rtable,
        gather_sensitive_relations_walker,
        relations,
        QTW_EXAMINE_RTES_BEFORE);
  }

  return false;
}

List *gather_sensitive_relations(Query *query)
{
  List *relations = NIL;
  gather_sensitive_relations_walker((Node *)query, &relations);
  return relations;
}
