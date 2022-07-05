-- Data analysis utility methods to guide admins in correctly configuring Diffix.

-- Inspects all columns in a table for pre-anonymization filtering safety.
-- Warns when a column should be rejected from filtering in untrusted-mode.
CREATE OR REPLACE PROCEDURE analyze_filterability(table_name text)
AS $$
  DECLARE
    rows_count bigint;
    column_name text;
    top_occurences bigint;
  BEGIN
    EXECUTE 'SELECT COUNT(*) FROM ' || table_name INTO STRICT rows_count;
    RAISE INFO 'Analyzing filterability of table `%` with % rows...', table_name, rows_count;

    FOR column_name IN EXECUTE 'SELECT attname FROM pg_attribute
          WHERE attrelid = ' || table_name::regclass::oid || ' AND attnum > 0 AND NOT attisdropped'
        LOOP
      RAISE INFO 'Inspecting column `%`...', column_name;
      EXECUTE 'SELECT COUNT(' || column_name || ') AS occurences FROM ' || table_name ||
        ' GROUP BY ' || column_name || ' ORDER BY occurences DESC LIMIT 1' INTO STRICT top_occurences;
      IF top_occurences >= 0.65 * rows_count THEN -- warn if top value occurs in more than 65% of rows
        RAISE WARNING 'Column `%` is dominated by one value and should be marked as `not_filterable`!', column_name;
      END IF;
    END LOOP;

    RAISE INFO 'Filterability analysis is complete.';
  END;
$$ LANGUAGE plpgsql;
