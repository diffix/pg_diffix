#include "postgres.h"

#include "pg_diffix/utils.h"

seed_t hash_set_to_seed(const List *hash_set)
{
  ListCell *cell = NULL;
  seed_t accumulator = 0;
  foreach (cell, hash_set)
  {
    hash_t hash = (hash_t)lfirst(cell);
    accumulator ^= hash;
  }
  return accumulator;
}
