SELECT
  logtime,
  loguser,
  logdatabase,
  logpid,
  logthread,
  loghost,
  logsegment,
  SQLText AS queryText
FROM %.gp_query_log
ORDER BY length(SQLText) DESC