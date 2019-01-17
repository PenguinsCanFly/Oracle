--total open cursors by user
select sum(a.value) total_num_cursor, s.username 
from v$sesstat a, v$statname b, v$session s 
where a.statistic# = b.statistic#  
    and s.sid=a.sid and b.name = 'opened cursors current' 
    and s.username is not null 
    and s.username NOT IN ('USERNMAE')
    and s.status = 'ACTIVE'
group by s.username
order by sum(a.value) desc;


--open cursors for RELEASE
select a.value num_cursor, s.username,  s.sid, s.OSUSER, s.command, s.status, s.MACHINE, s.program, s.sql_id, s.SQL_EXEC_START, s.SQL_EXEC_ID, s.module, s.action, s.row_wait_block#, s.ROW_WAIT_ROW#, s.BLOCKING_SESSION_STATUS, s.FINAL_BLOCKING_SESSION_STATUS, s.WAIT_CLASS, s.WAIT_TIME, s.SECONDS_IN_WAIT, s.STATE
from v$sesstat a, v$statname b, v$session s 
where a.statistic# = b.statistic#  and s.sid=a.sid and b.name = 'opened cursors current' and s.username is not null 
and s.username = 'USERNMAE'
and status = 'ACTIVE'
--and a.value >0
order by a.value desc;

--running query by sid
select sid, user_name, sql_text 
from v$open_cursor 
where sid in (4059);

--release progress
select sid, user_name, sql_text 
from v$open_cursor 
where sid in (select s.sid from v$sesstat a, v$statname b, v$session s 
            where a.statistic# = b.statistic#  and s.sid=a.sid and b.name = 'opened cursors current' 
            and s.username in ('DBRELMANP','DBRELMANU') and status = 'ACTIVE' and a.value >0);

--------------------
--locked objects
--------------------
select o.object_name, s.sid, s.serial#, p.spid, s.program, s.username, s.machine,s.port, s.logon_time, sq.sql_fulltext 
from v$locked_object l, dba_objects o, v$session s, 
v$process p, v$sql sq 
where l.object_id = o.object_id 
and l.session_id = s.sid 
and s.paddr = p.addr 
and s.sql_address = sq.address
--and s.sid = 3156;

--kill
alter system kill session 'SID,SERIAL#';
alter system kill session '3156,16537';

--kill all session that lock (-------do not do it----------)
begin
    for c in (select s.sid sid, s.serial# serial from v$locked_object l, v$session s where l.session_id = s.sid) loop
       execute immediate ('ALTER SYSTEM KILL SESSION ''' || c.sid || ',' || c.serial || '''');
    end loop;
end;


select * from DBA_ERRORS where owner = 'SCHEMA'
