# Changelog
  - Allow more metadata discovery queries.

## Version 1.0.3
  - Reject unsupported column types during AID labeling.

## Version 1.0.2
  - Allow casts between `int4` and `int8`.
  - Allow more metadata discovery queries.
  - Allow more statement types.
  - Fixed handling of `IN (subquery)` expressions.
  - Added analyst guide section for allowed type conversions.
  - Small fixes to docs.
  - Added support for parameterized queries.
  - Queries selecting or grouping by non-generalized AID columns are now rejected.

## Version 1.0.1
  - Fixed some docs links.
  - Fixed setup script version.

## Version 1.0.0

- Vastly expanded docs.
- Fixed a noise generation bug.
- Salt is randomly generated per-database during extension setup.
- Simplified and improved utility methods.
- The low count filter is not seeded with bucket data anymore.
- Extension now skips non-associated databases when loaded globally.
- Fixed handling of post-anonymization conditions.

## Version 0.0.4

- Minor updates to docs and config terminology.

## Version 0.0.3

- Tweaked README file for better rendering on PGXN.
