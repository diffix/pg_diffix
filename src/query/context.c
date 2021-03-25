#include "postgres.h"
#include "catalog/namespace.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/table.h"
#include "utils/rel.h"

#include "pg_diffix/utils.h"
#include "pg_diffix/query/context.h"
#include "pg_diffix/auth.h"

static SensitiveRelation *create_sensitive_relation(Oid rel_oid, Oid namespace_oid, Index rel_index)
{
  SensitiveRelation *sensitive_rel = palloc(sizeof(SensitiveRelation));
  sensitive_rel->namespace_oid = namespace_oid;
  sensitive_rel->oid = rel_oid;
  sensitive_rel->index = rel_index;
  sensitive_rel->aids = NIL;

  Relation rel = table_open(rel_oid, AccessShareLock);
  TupleDesc rel_desc = RelationGetDescr(rel);

  for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
  {
    Form_pg_attribute att = TupleDescAttr(rel_desc, i);

    if (is_aid_column(rel_oid, att->attnum))
    {
      AnonymizationID *aid = palloc(sizeof(AnonymizationID));
      aid->attnum = att->attnum;
      aid->atttype = att->atttypid;
      aid->typmod = att->atttypmod;
      aid->collid = att->attcollation;

      sensitive_rel->aids = lappend(sensitive_rel->aids, aid);
    }
  }

  table_close(rel, NoLock);

  return sensitive_rel;
}

/* Returns a list (of DiffixRelation) of all relations in the query. */
static List *gather_sensitive_relations(Query *query)
{
  List *result = NIL; /* List with resulting DiffixRelation */

  ListCell *lc;
  foreach (lc, query->rtable)
  {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
    if (!OidIsValid(rte->relid))
      continue;

    Oid namespace_oid = get_rel_namespace(rte->relid);
    if (is_sensitive_relation(rte->relid, namespace_oid))
    {
      Index rel_index = foreach_current_index(lc) + 1;
      SensitiveRelation *rel_data = create_sensitive_relation(rte->relid, namespace_oid, rel_index);
      result = lappend(result, rel_data);
    }
  }

  return result;
}

QueryContext build_query_context(Query *query)
{
  QueryContext context = {
      .query = query,
      .relations = gather_sensitive_relations(query),
  };
  return context;
}
