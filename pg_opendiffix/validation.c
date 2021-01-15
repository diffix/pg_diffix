#include "postgres.h"

#include "pg_opendiffix/validation.h"
#include "pg_opendiffix/config.h"

/*
 * Returns true if query range contains any sensitive relation.
 * See config.h for relation configuration.
 */
bool requires_anonymization(Query *query)
{
  OpenDiffixConfig *config;
  ListCell *lc;

  /*
   * We don't care about non-SELECT queries.
   * Write permissions should be handled by other means.
   */
  if (query->commandType != CMD_SELECT)
  {
    return false;
  }

  config = get_opendiffix_config();

  foreach (lc, query->rtable)
  {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

    /* Check if relation OID is present in config. */
    if (rte->relid && get_relation_config(config, rte->relid) != NULL)
    {
      return true;
    }
  }

  /* No sensitive relations found in config. We consider it a regular query. */
  return false;
}

/*
 * Verifies that a query matches current anonymization restrictions and limitations.
 * If requirements are not met, an error is reported and execution is halted.
 */
void verify_anonymization_requirements(Query *query)
{
}
