select * from all_users where username = 'USERNAME';
SELECT username, account_status,lock_date, PROFILE FROM dba_users WHERE username='USERNAME';

alter user USERNAME account unlock;

ALTER USER [USERNAME] IDENTIFIED BY [paSSw0rd];

--add missing login
----------------------------------------------------------------
DROP USER USERNAME;

CREATE USER USERNAME 
  IDENTIFIED BY w3OBkF2e
  DEFAULT TABLESPACE SCHEMA_DAT
  QUOTA UNLIMITED ON SCHEMA_DAT;
  
create user USERNAME IDENTIFIED BY w3OBkF2e;
alter user USERNAME IDENTIFIED BY w3OBkF2e;


--grant dba to USERNAME with admin option;
GRANT CONNECT TO USERNAME;
GRANT all privileges ON schame.package_pkg TO USERNAME;
select p.* from dba_sys_privs p where grantee='USERNAME' order by 1;

--add role to user (add user to the groups)
GRANT role1_grp, role2_grp TO USERNAME;
----------------------------------------------------------------



----------------------------------------------------------------
---------------------
expired accounts fix
--------------------
SELECT username, account_status,lock_date, PROFILE FROM dba_users WHERE username='USERNAME';
select expiry_date from dba_users where username = 'USERNAME'; --18-08-18 02.10 PM
--EXPIRED --> OPEN
alter user USERNAME identified by values 'w31BkF2i';

