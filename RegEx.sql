--Find substrng in the middle ('12')
select REGEXP_SUBSTR('1,500,000 @ 100; Posted By: Somebody -> test; Posting Status: U->C; Posting Version: 12->43', 
'Posting Version:(.*?)->',1,1,null,1) as regex_sub from dual;

--Find substrng at the end ('67')
select regexp_substr('1,500,000 @ 100; Posted By: Adapter->test; Posting Status: U->C; Posting Version: 66->67', 
'Posting Version:.*->(.*?)$',1,1,null,1) as regex_sub from dual;


WITH test_data AS
(
    SELECT 'Joseph Bloggs (Joe) (THIS)' v1 FROM DUAL UNION ALL
    SELECT 'Robert Holnas (Bob) (THAT)' FROM DUAL UNION ALL
    SELECT 'Mary Mild (THIS)' FROM DUAL UNION ALL
    SELECT 'Jack Jill (THIS)' FROM DUAL
)
SELECT v1
     , regexp_substr(v1,'\((.*?)\)',1,1,null,1) bracket1
     , regexp_substr(v1,'\((.*?)\)',1,2,null,1) bracket2
FROM test_data;

V1                         BRACKET1                   BRACKET2
-------------------------- -------------------------- --------------------------
Joseph Bloggs (Joe) (THIS) Joe                        THIS
Robert Holnas (Bob) (THAT) Bob                        THAT
Mary Mild (THIS)           THIS
Jack Jill (THIS)           THIS


The regular expression finds anything between brackets (the ? makes it non-greedy, that is important.)
1 means start from the beginning of the string.
Next digit means that bracket1 is "first occurence", bracket2 is "second occurence" - you could continue with 3, 4, etc.
null just uses default options for reg exp (here could be option for example for case in-sensitive search.)
The final 1 means that the result is the first "group" in the reg exp - this gets us only the text within the brackets and not the brackets themselves.
