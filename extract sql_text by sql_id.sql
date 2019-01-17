select * from v$sqltext where sql_id like 'ckrnua4s833v8' order by piece;
select * from v$sqltext where sql_id = '7dgg8myktzk0n' order by piece;
select * from dba_source where lower(text) like '%select 1 from schema.tablename where column1 =%';
