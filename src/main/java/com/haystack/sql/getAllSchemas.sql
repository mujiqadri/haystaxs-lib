SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE ('pg_%') AND
      schema_name NOT LIKE ('gp_%') AND
      schema_name NOT IN ('information_schema', 'madlib');