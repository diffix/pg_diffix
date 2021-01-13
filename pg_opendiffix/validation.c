#include "postgres.h"

#include "pg_opendiffix/validation.h"

bool requires_anonymization(Query *query)
{
  return false;
}

void verify_anonymization_requirements(Query *query)
{
}
