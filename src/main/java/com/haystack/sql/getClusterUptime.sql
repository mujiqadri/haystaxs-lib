-- External Table: gp_toolkit.shutdownlog_ext

--
 DROP EXTERNAL TABLE IF EXISTS gp_toolkit.shutdownlog_ext;

CREATE EXTERNAL WEB TABLE gp_toolkit.shutdownlog_ext
(
  line text
)
 EXECUTE E'cat /home/gpadmin/gpAdminLogs/gpstop_*.log' ON MASTER
 FORMAT 'text' (delimiter ';' null E'\\N' escape E'\x09' fill missing fields )
ENCODING 'UTF8';

DROP EXTERNAL TABLE IF EXISTS gp_toolkit.startuplog_ext;

CREATE EXTERNAL WEB TABLE gp_toolkit.startuplog_ext
(
  line text
)
 EXECUTE E'cat /home/gpadmin/gpAdminLogs/gpstart_*.log' ON MASTER
 FORMAT 'text' (delimiter '	' null '\\N' escape '\\' fill missing fields newline 'LF')
ENCODING 'UTF8';

select x.time as StartTime, y.time as shutdowntime, (x.time - y.time)::INTERVAL
from (
	select (substring(line from 1 for 8) || ' ' || substring(line from 10 for 8) || '.' || substring(line from 19 for 6))::timestamp as time, substring(line from 26 for length(line)-25) as msg
	from  gp_toolkit.startuplog_ext start
	where line like '%gpstart:%' and line like '%Database%started%'
	) as X,
    (
	select (substring(line from 1 for 8) || ' ' || substring(line from 10 for 8) || '.' || substring(line from 19 for 6))::timestamp as time, substring(line from 26 for length(line)-25) as msg
	from gp_toolkit.shutdownlog_ext
	where line  like '%gpstop:%' and line like '%Database%shutdown%'
	) as Y
where X.time > Y.time
order by X.time , Y.time


---=====


select x.time as StartTime, y.time as shutdowntime, (x.time - y.time)::INTERVAL
from (
	select (substring(line from 1 for 8) || ' ' || substring(line from 10 for 8) || '.' || substring(line from 19 for 6))::timestamp as time, substring(line from 26 for length(line)-25) as msg
	from  gp_toolkit.startuplog_ext start
	where line like '%gpstart:%' and line like '%Database%started%'
	) as X,
    (
	select (substring(line from 1 for 8) || ' ' || substring(line from 10 for 8) || '.' || substring(line from 19 for 6))::timestamp as time, substring(line from 26 for length(line)-25) as msg
	from gp_toolkit.shutdownlog_ext
	where line  like '%gpstop:%' and line like '%Database%shutdown%'
	) as Y
where X.time > Y.time
order by X.time , Y.time

----------
DROP TYPE gp_toolkit.uptimerecord;
CREATE TYPE gp_toolkit.uptimerecord AS (
  event text,
  starttime timestamp,
  endtime timestamp,
  duration interval
);

select gp_toolkit.uptime('2015-09-01'::timestamp);

DROP FUNCTION gp_toolkit.uptime(timestamp);

CREATE OR REPLACE FUNCTION gp_toolkit.uptime( timestamp)
  RETURNS setof gp_toolkit.uptimerecord AS $$
DECLARE
  sinceDate ALIAS FOR $1;
  r  gp_toolkit.uptimerecord%rowtype;
BEGIN

	for i in 1..3 loop
		r.event := 'STARTUP';
		r.starttime :='2015-09-30 01:31:16.026447'::timestamp;
		r.endtime := '2015-09-30 01:31:16.026447'::timestamp;
		r.duration := '00:01:24.000172'::interval;
		return next r;
	end loop;
	return;

  --insert into returnrecords('start','2015-09-30 01:31:16.026447'::timestamp,'2015-09-30 01:31:16.026447'::timestamp ,'00:01:24.000172'::interval);
  --insert into returnrecords('start','2015-09-30 01:31:16.026447'::timestamp,'2015-09-30 01:31:16.026447'::timestamp ,'00:01:24.000172'::interval);

END;
$$ language plpgsql;
