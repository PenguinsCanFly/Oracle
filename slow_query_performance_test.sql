
-----------------------
-- you've got SQL_ID
-----------------------
select distinct
    sh.top_level_sql_id, --fnnrabt4gxtb2
    sh.sql_id,
    sh.instance_number,
    sh.session_id,
    sh.sql_plan_hash_value,
    sh.sql_exec_start,
    sh.program,
    sh.module,
    sh.action,
    sh.machine,
    u.username
from dba_users u
join dba_hist_active_sess_history sh on sh.user_id = u.user_id
where sql_id = '33yzg4cxttb58'
and u.username = 'USER1'
and sh.snap_id >= (select min(snap_id) from dba_hist_snapshot where begin_interval_time > systimestamp - interval '24' hour)
order by sh.sql_exec_start;

-- the list of execution plans generated for a particular SQL
select distinct dbid, sql_id, plan_hash_value, timestamp from dba_hist_sql_plan where sql_id = '33yzg4cxttb58';


-- To see the details of a particular execution plan:
SELECT * FROM table
(
  DBMS_XPLAN.DISPLAY_AWR
  (
    sql_id =>'33yzg4cxttb58',
    plan_hash_value =>2581113495,
    db_id =>3938360858,
    format => 'TYPICAL'
  )
);

