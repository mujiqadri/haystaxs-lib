-- Function: haystack_ui.create_ast_tables(text, boolean)

-- DROP FUNCTION haystack_ui.create_ast_tables(text, boolean);

CREATE OR REPLACE FUNCTION haystack_ui.create_ast_tables(userschema TEXT, recreate BOOLEAN)
  RETURNS VOID AS
  $BODY$
  DECLARE
    sql            TEXT;
    ast_exists     BOOLEAN;
    ast_qry_exists BOOLEAN;

  BEGIN
    IF (reCreate)
    THEN
      sql := 'drop table if exists ' || userschema || '.ast;';
      EXECUTE sql;
      sql := 'drop table if exists ' || userschema || '.ast_queries;';
      EXECUTE sql;
    END IF;
-- CREATE AST TABLE IF IT DOESNT EXIST
    IF ((SELECT count(*)
         FROM information_schema.tables
         WHERE upper(table_schema) = upper(userschema) AND upper(table_name) = upper('ast')) <= 0)
    THEN

      RAISE NOTICE 'AST TABLE DOESNOT EXIST: CREATING TABLE';

      sql := 'CREATE TABLE ' || userschema || '.ast (
				  ast_id serial NOT NULL,
				  ast_json text,
				  checksum text

			)
			WITH (APPENDONLY=true, COMPRESSTYPE=quicklz,
			  OIDS=FALSE
			)DISTRIBUTED BY (ast_id);';
      EXECUTE sql;
    END IF;

-- CREATE AST_QUERIES TABLE IF IT DOESNT EXIST
    IF ((SELECT count(*)
         FROM information_schema.tables
         WHERE upper(table_schema) = upper(userschema) AND upper(table_name) = upper('ast_queries')) <= 0)
    THEN

      RAISE NOTICE 'AST_QUERIES TABLE DOESNOT EXIST: CREATING TABLE';

      sql := 'CREATE TABLE ' || userschema || '.ast_queries
			(
			  ast_queries_id serial NOT NULL,
			  queries_id integer NOT NULL,
			  ast_json text,
			  checksum text,
			  ast_id integer NOT NULL
			)
			WITH (APPENDONLY=true, COMPRESSTYPE=quicklz,
			  OIDS=FALSE
			)
			DISTRIBUTED BY (queries_id);';
      EXECUTE sql;
    END IF;
  END;
  -- outer function wrapper
  $BODY$
LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION haystack_ui.create_ast_tables( TEXT, BOOLEAN )
OWNER TO gpadmin;
