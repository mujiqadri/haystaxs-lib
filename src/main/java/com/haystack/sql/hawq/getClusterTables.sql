select  table_schema, table_name, table_catalog
from information_schema.tables
where table_type = 'BASE TABLE'
      and is_insertable_into = 'YES'
      and table_schema not in ('pg_catalog', 'information_schema','madlib','hawq_toolkit')
order by table_schema, table_name