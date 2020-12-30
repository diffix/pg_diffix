#include "postgres.h"

/* Function manager. */
#include "fmgr.h"

/* Post parse analyze hook */
#include "parser/analyze.h"

/* Planner hook */
#include "optimizer/planner.h"

/* PG extension setup */

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

/* Hooks */

static void pg_opendiffix_post_parse_analyze(ParseState *pstate, Query *query);
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

static PlannedStmt *pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);
static planner_hook_type prev_planner_hook = NULL;

/* Definitions */

void
_PG_init(void)
{
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_opendiffix_post_parse_analyze;

	prev_planner_hook = planner_hook;
	planner_hook = pg_opendiffix_planner;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	planner_hook = prev_planner_hook;
}

static void
pg_opendiffix_post_parse_analyze(ParseState *pstate, Query *query)
{
	ereport(LOG, (errmsg("parsed a query %s", pstate->p_sourcetext)));
}

static PlannedStmt *
pg_opendiffix_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	return standard_planner(parse, query_string, cursorOptions, boundParams);
}
