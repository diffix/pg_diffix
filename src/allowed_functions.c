#include "postgres.h"
#include "utils/memutils.h"

#include "pg_diffix/allowed_functions.h"

IntegerSet *g_allowed_functions = NULL;

static bool g_loaded = false;

void allowed_functions_init(void)
{
  if (g_loaded)
    return;

  MemoryContext old_memory_context = MemoryContextSwitchTo(TopMemoryContext);

  g_allowed_functions = intset_create();
  // float<->int casts
  intset_add_member(g_allowed_functions, 235);
  intset_add_member(g_allowed_functions, 236);
  intset_add_member(g_allowed_functions, 237);
  intset_add_member(g_allowed_functions, 238);
  intset_add_member(g_allowed_functions, 311);
  intset_add_member(g_allowed_functions, 312);
  intset_add_member(g_allowed_functions, 313);
  intset_add_member(g_allowed_functions, 314);
  intset_add_member(g_allowed_functions, 316);
  intset_add_member(g_allowed_functions, 317);
  intset_add_member(g_allowed_functions, 318);
  intset_add_member(g_allowed_functions, 319);
  // width_bucket
  intset_add_member(g_allowed_functions, 320);
  // more float<->int casts
  intset_add_member(g_allowed_functions, 482);
  intset_add_member(g_allowed_functions, 483);
  intset_add_member(g_allowed_functions, 652);
  intset_add_member(g_allowed_functions, 653);
  // substring
  intset_add_member(g_allowed_functions, 877);
  intset_add_member(g_allowed_functions, 936);
  // numeric casts
  intset_add_member(g_allowed_functions, 1740);
  intset_add_member(g_allowed_functions, 1742);
  intset_add_member(g_allowed_functions, 1743);
  intset_add_member(g_allowed_functions, 1744);
  intset_add_member(g_allowed_functions, 1745);
  intset_add_member(g_allowed_functions, 1746);

  MemoryContextSwitchTo(old_memory_context);

  g_loaded = true;
}

void allowed_functions_cleanup()
{
  if (!g_loaded)
    return;
  pfree(g_allowed_functions);
  g_loaded = false;
}
