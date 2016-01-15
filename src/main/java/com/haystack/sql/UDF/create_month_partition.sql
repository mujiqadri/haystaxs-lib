-- Function: haystack_ui.create_month_partition(text, text, text)

-- DROP FUNCTION haystack_ui.create_month_partition(text, text, text);

CREATE OR REPLACE FUNCTION haystack_ui.create_month_partition(schema_name         TEXT, table_name TEXT,
                                                              monthpartition_name TEXT)
  RETURNS BOOLEAN AS
  $BODY$
  DECLARE
    mycommand             TEXT;
    monthpartition_exists BOOLEAN;

  BEGIN
-- Find how many days in the current month
-- query to see if the target partition already exists

    SELECT count(*) > 0
    INTO monthpartition_exists
    FROM pg_partitions
    WHERE partitionname = monthpartition_name
          AND tablename = table_name
          AND schemaname = schema_name;

-- if the target partition does not exist create it and return

    IF monthpartition_exists = 't'
    THEN
      RAISE INFO 'Month Partition Already Exists';
      RETURN FALSE;
    END IF;

-- if the target partition does not exist create it and return
    IF monthpartition_exists = 'f'
    THEN

      mycommand :=
      'ALTER TABLE ' || schema_name || '.' || table_name || ' ADD PARTITION "' || $3 || '" START (''' || $3 ||
      ' 00:00:00.000'') '
      || ' INCLUSIVE END (''' || $3 || ' 23:59:59.999'') EXCLUSIVE;';
      RAISE INFO 'Month partition does not exist.  Creating partition now using [%]', mycommand;
      EXECUTE mycommand;

    END IF;

    RETURN TRUE;

  END;

  $BODY$
LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION haystack_ui.create_month_partition( TEXT, TEXT, TEXT )
OWNER TO gpadmin;
