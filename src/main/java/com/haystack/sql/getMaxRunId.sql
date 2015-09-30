SELECT coalesce(max(run_id) + 1, 1) AS max_run_id
FROM haystack.run_log;

