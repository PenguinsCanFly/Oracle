select * from dba_errors where owner in ('SCHEMA_NAME');

--------------------
--running queries
--------------------
select o.object_name, s.sid, s.serial#, p.spid, s.program, s.username, s.machine,s.port, s.logon_time, sq.sql_fulltext 
from 
    dba_objects o, 
    gv$session s, 
    gv$process p, 
    gv$sql sq 
where 
    s.paddr = p.addr 
    and s.sql_address = sq.address
    and s.username = 'USERNAME'
    and o.owner = 'SCHEMA'
;


--------------------
--locked objects
--------------------
select o.object_name, s.sid, s.serial#, p.spid, s.program, s.username, s.machine,s.port, s.logon_time, sq.sql_fulltext 
from 
    gv$locked_object l, 
    dba_objects o, 
    gv$session s, 
    gv$process p, 
    gv$sql sq 
where 
    l.object_id = o.object_id 
    and l.session_id = s.sid 
    and s.paddr = p.addr 
    and s.sql_address = sq.address
    and s.username = 'USERNAME'
    and o.owner = 'TRADEREPOSITORY'
;

----------------------------
--Check one table for locks
----------------------------
select o.object_name, s.sid, s.serial#, p.spid, s.program, s.username, s.machine,s.port, s.logon_time, sq.sql_fulltext 
from 
    gv$locked_object l, 
    dba_objects o, 
    gv$session s, 
    gv$process p, 
    gv$sql sq 
where 
    l.object_id = o.object_id 
    and l.session_id = s.sid 
    and s.paddr = p.addr 
    and s.sql_address = sq.address
    and s.username = 'USER'
    --and o.object_name = 'TABLE';
    and o.owner = 'SCHEMA';

-----------------------
-- uncommited sessions
------------------------
select s.sid
      ,s.serial#
      ,s.username
      ,s.machine
      ,s.status
      ,s.lockwait
      ,t.used_ublk
      ,t.used_urec
      ,t.start_time
from v$transaction t
inner join v$session s on t.addr = s.taddr;

-----------------------
-- ddl locks
------------------------

select * from dba_ddl_locks where owner = 'SCHEMA' and mode_held <> 'Null';
select * from dba_ddl_locks where owner = 'SCHEMA' and mode_held <> 'Null';
select * from v$session where sid = 5700;

grant alter system to [user_name] ;

--kill
alter system kill session 'SID,SERIAL#';
alter system kill session '3677,13889';
alter system kill session '4302,42901';



--kill all session that lock (-------do not do it----------)
begin
    for c in (select s.sid sid, s.serial# serial from v$locked_object l, v$session s where l.session_id = s.sid) loop
       execute immediate ('ALTER SYSTEM KILL SESSION ''' || c.sid || ',' || c.serial || '''');
    end loop;
end;




--who blocks my table?
select o.object_name, s.* , l.*, o.*
from v$locked_object l
inner join dba_objects o on l.object_id = o.object_id
inner join gv$session s on l.session_id = s.sid and o.object_name = 'TABLE NAME';

--kill it
alter system kill session 'SID,SERIAL#';
alter system kill session '2,5441';

--get details

-- the list of execution plans generated for a particular SQL
select distinct dbid, sql_id, plan_hash_value from dba_hist_sql_plan where sql_id = '6fvrc03cksrz3';

-- To see the details of a particular execution plan:
prod db_id 3938360858
uat db_id 4171397931
SELECT * FROM table
(
  DBMS_XPLAN.DISPLAY_AWR
  (
    sql_id =>'6fvrc03cksrz3',
    plan_hash_value =>2191140404,
    db_id =>4171397931,
    format => 'TYPICAL'
  )
);

