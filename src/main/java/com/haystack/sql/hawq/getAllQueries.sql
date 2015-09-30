SELECT
  logtime,
  loguser,
  logdatabase,
  logpid,
  logthread,
  loghost,
  logsegment,
  logdebug AS SQLText, *
FROM hawq_toolkit.__hawq_log_master_ext
WHERE logmessage like 'statement:%'
      AND lower(logmessage) like '%select%from%'
      AND logdatabase NOT IN ('template1', 'postgres', 'gpperfmon')
      AND logdatabase NOT like 'template%'
      AND logdebug not like '%pg_type%'
      AND logdebug not like '%pg_database%'
      AND logdebug not like '%pg_namespace%'
      AND logdebug not like '%pg_attribute%'
      AND logdebug not like '%pg_catalog%'
      AND logdebug not like '%pg_trigger%'
      AND logdebug not like '%::%'
      AND logtime > ?