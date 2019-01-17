/*
1) Session (client) 
    the timezone of the session/client 
    Shown in SESSIONTIMEZONE
    This is the timezone of CURRENT_DATE, LOCALTIMESTAMP and CURRENT_TIMESTAMP. The difference between those 3 is the return type, they return a DATE, TIMESTAMP, and TIMESTAMP WITH TIME ZONE respectively) 
2) The database timezone 
    Shown in DBTIMEZONE
    This is the the timezone used for the internal storage of TIMESTAMP WITH LOCAL TIME ZONE values. Note that values are converted to/from session timezone on insert/select so it actually isn't as important as it seems
    This is NOT the timezone of SYSDATE/SYSTIMESTAMP
3) The database OS timezone 
    In unix, it is based on the TZ variable when Oracle is started
    This is the timezone of SYSDATE and SYSTIMESTAMP
*/


--alter session set time_zone='-04:00';

select substr(to_char(to_timestamp('20170811094631934993','YYYYMMDDHH24MISSFF'),'YYYYMMDD-HH24:MI:SS.FF'),1,21) from dual
select to_timestamp('20170811094631934993','YYYYMMDDHH24MISSFF') from dual
    
need this format:
yyyyMMdd-HH:mm:ss.SSS 


SELECT  
  TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI')                        as sysdt
  ,TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD HH24:MI')                  as curr_dt
  ,TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZR')        as sys_ts
  ,TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')   as curr_ts
  ,EXTRACT(timezone_abbr FROM CURRENT_TIMESTAMP)                as tz_nm
  ,TO_CHAR(LOCALTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZR')      as local_ts
  ,DBTIMEZONE
  ,SESSIONTIMEZONE
  ,TO_CHAR(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS TZR') as as_UTC
FROM DUAL;

--print TZ only
select to_char(CURRENT_TIMESTAMP, 'TZH') from dual;

--different TZ formats
SELECT
  TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZR')      as curr_ts1  --2016-05-25 09:24:08.387755 AMERICA/NEW_YORK
  ,TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') as curr_ts2  --2016-05-25 09:24:08.387755 -04:00
FROM DUAL;

--convert TIMESTAMP from one TZ to another
SELECT 
  FROM_TZ(CAST(TO_DATE('1999-12-01 11:00:00', 'YYYY-MM-DD HH:MI:SS') AS TIMESTAMP), 'America/New_York') 
  AT TIME ZONE 'America/Los_Angeles' "West Coast Time" 
FROM DUAL;

--convert DATE from one timezone to another
SELECT TO_CHAR(NEW_TIME(TO_DATE('2016-05-27 22:15:01', 'YYYY-MM-DD HH24:MI:SS'), 'EST', 'PST'),'YYYY-MM-DD HH24:MI') tm from dual;
SELECT TO_TIMESTAMP(NEW_TIME(TO_DATE('2016-05-27 22:15:01', 'YYYY-MM-DD HH24:MI:SS TZR'), 'AST', 'MST'),'YYYY-MM-DD HH24:MI TZR') tm from dual;
-- convert to UTC
select sys_extract_utc(systimestamp) from dual;

--convert local server time to BST to send downstream
SELECT 
  SYSTIMESTAMP as db_sever_tm,
  SYSTIMESTAMP AT TIME ZONE 'Europe/London' as BST,
  EXTRACT(timezone_abbr FROM SYSTIMESTAMP AT TIME ZONE 'Europe/London') as abbr
FROM DUAL;

-- all the time zone names 
SELECT tzname, tzabbrev FROM v$timezone_names; --2164 rows
SELECT tzname, tzabbrev FROM v$timezone_names WHERE tzname like '%New%';
SELECT tzname, tzabbrev FROM v$timezone_names WHERE tzabbrev = 'BST';

--------------------------------------
--update table set all values at UTC
--------------------------------------
create table foo( col1 timestamp with time zone default current_timestamp);
insert into foo values( current_timestamp );
update foo set col1 = col1 at time zone 'UTC';


-----------------
--DATE PART
----------------
select TRUNC(TO_DATE('27-OCT-92','DD-MON-YY'), 'YEAR') from dual;
select SYSDATE, TRUNC(SYSDATE) from dual;


-----------------------------------------------
-------convert from string to date
-----------------------------------------------
select to_date('2016-05-25','YYYY-MM-DD') from Dual;
select to_timestamp_tz('1970-01-01 00:00:00.000 +00:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') ts_tz from Dual;



--move from one time zone to another 
select
    cast(t.exec_tm as timestamp) t1, --wrong
    cast(t.exec_tm as timestamp with time zone) t2, --wrong
    t.exec_tm at time zone 'GMT' as exec_tm_tz, --- correct
from  schema.table ta 
where trade_dt = to_date('02/01/2017', 'MM/DD/YYYY')
    

---load timestamp from db and add timezone to it
select 
    e.executed_tm,
    from_tz(e.executed_tm, 'GMT') at time zone 'GMT' as t_gmt, --- 12.15 gmt
    from_tz(e.executed_tm, 'GMT') at time zone 'America/New_York' as t_est, --- 7.15 est
    from_tz(e.executed_tm, 'Europe/London') at time zone 'EST' as t_bst --- 6.15 est
from  schema.table;

--where e.executed_dt = to_date('02/01/2017', 'MM/DD/YYYY')


--from timestamp to date
select l.load_id, cast(l.from_tm as date) as from_dt, cast(l.snapshot_to_tm as date) as to_dt 
from  schema.table;
