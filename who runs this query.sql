--find the query by text
select * from dba_hist_sqltext where sql_text like '% text text text %';

sql_id = fpngdxymz42j4
db_id=3938360858


--find query by sql_id
select * from dba_hist_sqltext where DBID = 3938360858 and sql_id = 'fpngdxymz42j4';


--select s.* from v$session s where s.sql_id = 'fpngdxymz42j4';

--find the open cursor by sql_id
select c.* from v$open_cursor c where c.sql_id = 'fpngdxymz42j4';


select c.* from v$open_cursor c where upper(c.sql_text) like '%text text text%';

not sure if this works, never tested
