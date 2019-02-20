--Find substrng in the middle ('12')
select REGEXP_SUBSTR('1,500,000 @ 100; Posted By: Somebody -> test; Posting Status: U->C; Posting Version: 12->43', 
'Posting Version:(.*?)->',1,1,null,1) as regex_sub from dual;

--Find substrng at the end ('67')
select regexp_substr('1,500,000 @ 100; Posted By: Adapter->test; Posting Status: U->C; Posting Version: 66->67', 
'Posting Version:.*->(.*?)$',1,1,null,1) as regex_sub from dual;
