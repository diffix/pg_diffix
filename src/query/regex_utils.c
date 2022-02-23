#include "postgres.h"

#include "pg_diffix/query/regex_utils.h"
#include "pg_diffix/utils.h"

#include <regex.h>

static regex_t g_generalization_regex;

void regex_init(void)
{
  const char *pattern = "^[125]\\.0+e[-+][0-9]+$";
  if (regcomp(&g_generalization_regex, pattern, REG_EXTENDED + REG_NOSUB) != REG_NOERROR)
    FAILWITH("Could not compile g_generalization_regex");
}

void regex_cleanup(void)
{
  regfree(&g_generalization_regex);
}

bool generalization_regex_match(const char *string)
{
  return regexec(&g_generalization_regex, string, 0, NULL, 0) == REG_NOERROR;
}
