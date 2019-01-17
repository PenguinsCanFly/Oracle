SELECT * FROM dba_profiles WHERE PROFILE='DEFAULT' ORDER BY resource_name;
SELECT * FROM dba_audit_session ORDER BY sessionid DESC;

SELECT * FROM dba_audit_session WHERE username='USERNAME' and timestamp >= date '2017-10-25' and returncode=1017 order by timestamp desc;
SELECT * FROM dba_audit_session WHERE username='USERNAME' and timestamp >= date '2018-06-13' and returncode=1017 order by timestamp desc;
SELECT * FROM dba_audit_session WHERE username='USERNAME' and timestamp >= date '2018-06-05' and returncode=1017 order by timestamp desc;

SELECT * FROM dba_audit_session WHERE timestamp >= date '2017-10-20' and returncode=1017 order by timestamp desc;


username,       userhost,           returncode
USERNAME	         xxx\host	        1017
USERNAME	         xxx\host	           1017

SELECT username, account_status,lock_date, PROFILE FROM dba_users WHERE username='USERNAME';


SELECT os_username, username, userhost, max(timestamp) tm FROM dba_audit_session WHERE username='USERNAME' and timestamp >= date '2017-10-23' group by os_username, username, userhost
ALTER USER USERNAME ACCOUNT UNLOCK;

