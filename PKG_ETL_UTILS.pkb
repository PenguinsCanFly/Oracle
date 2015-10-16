prompt Creating package body PKG_ETL_UTILS

CREATE OR REPLACE PACKAGE BODY resultsstaging.pkg_etl_utils AS
/*
 Package for performing ETL tasks
 
 History of changes (newest to oldest):
 ------------------------------------------------------------------------------
 21-Aug-2015, OK: added debugging support;
 28-Jul-2015, OK: changed calls to XL procedures;
 04-Jun-2015, OK: created;
*/

-- =================================================
--  Newly added debugging support:
  TYPE debug_rec IS RECORD
  (
    action    VARCHAR2(256),
    start_ts  TIMESTAMP
  );
  
  TYPE debug_array IS TABLE OF debug_rec INDEX BY PLS_INTEGER; 
  
  action_stack    debug_array;
  g_act_ptr       PLS_INTEGER; -- Action stack pointer
  
  -- Procedure to open a log - creates entry in LOAD table
  PROCEDURE open_log(i_name IN VARCHAR2, i_description IN VARCHAR2) IS
  BEGIN
    IF g_proc_id IS NULL THEN
      pkg_log_utility.prc_create_load
      (
        i_loadUID => g_proc_id,
        i_loadType => i_name,
        i_loadDesc => i_description
      );
    
      g_act_ptr := 0;
    END IF;
  END;
  
  -- Procedure to close the log - updates the current entry in LOAD table
  PROCEDURE close_log(i_result IN VARCHAR2) IS
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      pkg_log_utility.prc_update_load(g_proc_id, i_result);
    END IF;
    g_proc_id := NULL;
  END;
  
  -- Procedure to add an entry into EVENTLOG table
  PROCEDURE write_log
  (
    i_action    IN VARCHAR2,
    i_comment   IN VARCHAR2,
    i_start_ts  IN TIMESTAMP,
    i_end_ts    IN TIMESTAMP DEFAULT NULL
  ) IS
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      pkg_log_utility.prc_create_eventlog
      (
        i_LoadUID => g_proc_id,
        i_AppName => 'RDA', 
        i_StepName => g_procedure_name, 
        i_SubStepName => i_action, 
        i_EventDesc => i_comment,
        i_StartTime => i_start_ts,
        i_EndTime => i_end_ts
      );
    END IF;
  END;
  
  -- To be called BEFORE starting some action that needs logging.
  PROCEDURE begin_action(i_action IN VARCHAR2, i_comment IN VARCHAR2 DEFAULT 'Started') IS
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      g_act_ptr := g_act_ptr+1;
      action_stack(g_act_ptr).action := i_action;
      action_stack(g_act_ptr).start_ts := SYSTIMESTAMP; 
      write_log(i_action, i_comment, action_stack(g_act_ptr).start_ts);
    END IF; 
  END;
  
  -- To be called AFTER completing the action.
  PROCEDURE end_action(i_comment IN VARCHAR2 DEFAULT 'Done') IS
  BEGIN
    IF g_proc_id IS NOT NULL THEN
      write_log(action_stack(g_act_ptr).action, i_comment, action_stack(g_act_ptr).start_ts, SYSTIMESTAMP); 
      g_act_ptr := g_act_ptr-1;
    END IF; 
  END;
-- End of newly added debugging support
-- =================================================

  -- Procedure RESOLVE_NAME resolves the given table/view/synonym name
  -- into complete description of the underlying table/view:
  -- schema, table/view name, DB link
  PROCEDURE resolve_name
  (
    i_name    IN  VARCHAR2,
    o_schema  OUT VARCHAR2,
    o_table   OUT VARCHAR2,
    o_db_link OUT VARCHAR2
  ) IS
    l_name  VARCHAR2(92);
    l       PLS_INTEGER;
    n       PLS_INTEGER;
    m       PLS_INTEGER;
  BEGIN
    l_name := UPPER(i_name);
    n := INSTR(l_name, '.');
    m := INSTR(l_name, '@');
    l := LENGTH(l_name);

    IF n>0 OR m>0 THEN
      IF n>0 THEN
        o_schema := SUBSTR(l_name, 1, n-1);
        o_table := SUBSTR(l_name, n+1, CASE WHEN m>0 THEN m-n-1 ELSE l END);
        o_db_link := CASE WHEN m>0 THEN SUBSTR(l_name, m) END;
      ELSE
        o_table := SUBSTR(l_name, 1, m-1);
        o_db_link := SUBSTR(l_name, m);
        SELECT username INTO o_schema FROM user_db_links WHERE db_link = UPPER(SUBSTR(o_db_link, 2));
      END IF;

    ELSE
      SELECT table_owner, table_name, db_link
      INTO o_schema, o_table, o_db_link
      FROM
      (
        SELECT
          SYS_CONTEXT('USERENV','CURRENT_SCHEMA') table_owner,
          object_name table_name,
          NULL db_link
        FROM user_objects
        WHERE object_type IN ('TABLE','VIEW') AND object_name = UPPER(i_name)
        UNION ALL
        SELECT
          table_owner,
          table_name,
          NVL2(db_link, '@'||db_link, NULL) db_link
        FROM user_synonyms
        WHERE synonym_name = UPPER(i_name)
      );
    END IF;
  EXCEPTION
   WHEN NO_DATA_FOUND THEN
    Raise_Application_Error(-20000, 'Unknown table/view: '||i_name);
  END;
  
  
  -- Function GET_COL_LIST returns a comma-separated list of all the table column names.
  FUNCTION get_col_list(i_table IN VARCHAR2) RETURN VARCHAR2 IS
    l_schema  VARCHAR2(30);
    l_tname   VARCHAR2(30);
    l_dblink  VARCHAR2(100);
    ret       VARCHAR2(1000);
  BEGIN
    resolve_name(i_table, l_schema, l_tname, l_dblink);
    
    SELECT fnc_concat_v2_set
    (
      CURSOR
      (
        SELECT column_name
        FROM all_tab_cols
        WHERE owner = l_schema
        AND table_name = l_tname
        ORDER BY column_id
      )
    ) INTO ret FROM dual;
          
    RETURN ret;
  END;
  
  -- Function GET_KEY_COL_LIST returns a comma-separated list of the table key column names.
  -- By default, describes the table PK.
  -- Optionally, can describe the given UK, 
  FUNCTION get_key_col_list(i_table IN VARCHAR2, i_key IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS
    l_schema  VARCHAR2(30);
    l_tname   VARCHAR2(30);
    l_dblink  VARCHAR2(100);
    ret       VARCHAR2(1000);
  BEGIN
    resolve_name(i_table, l_schema, l_tname, l_dblink);
    
    SELECT fnc_concat_v2_set
    (
      CURSOR
      (
        SELECT cc.column_name
        FROM all_constraints c
        JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
        WHERE c.owner = l_schema AND c.table_name = l_tname 
        AND
        (
          c.constraint_type = 'P' AND i_key IS NULL
          OR
          c.constraint_name = i_key
        )
        ORDER BY cc.position
      )
    ) INTO ret FROM dual;
          
    RETURN ret;
  END;
  
  -- Function GET_COLUMN_INFO returns a table-like structure with descriptions of all the table columns.
  -- See definition of the type RESULTSSTAGING.OBJ_COLUMN_INFO.
  FUNCTION get_column_info(i_table IN VARCHAR2) RETURN tab_column_info PIPELINED IS
    l_schema  VARCHAR2(30);
    l_tname   VARCHAR2(30);
    l_dblink  VARCHAR2(100);
  BEGIN
    resolve_name(i_table, l_schema, l_tname, l_dblink);
    
    FOR r IN
    (
      SELECT * FROM vw_all_columns
      WHERE owner = l_schema AND table_name = l_tname
      ORDER BY column_id  
    )
    LOOP
      PIPE ROW(obj_column_info
      (
        r.owner, r.table_name, r.column_id, r.column_name,
        r.data_type, r.data_length, r.data_precision, r.data_scale, 
        r.nullable, r.pk
      ));
    END LOOP;
  END;
  
  -- Procedure ADD_DATA selects data from the specified source table or view (I_SRC)
  -- using optional WHERE (I_WHR) condition.
  -- Depending on I_OPERATION, it either merges or inserts the source data into the Target table (I_TGT).
  -- The output parameter O_ADD_CNT gets the number of rows added to the target table.
  -- O_ERR_CNT gets the number of source rows that have been rejected and placed in the error table (O_ERRTAB).
  -- Note: if O_ERRTAB is not specified, then this procedure errors-out
  -- if at least one source row cannot be placed in the target table.
  PROCEDURE add_data
  (
    i_operation     IN VARCHAR2, -- 'INSERT', 'UPDATE', 'MERGE', 'APPEND', 'REPLACE' or 'EQUALIZE'
    i_tgt           IN VARCHAR2, -- target table to add rows to
    i_src           IN VARCHAR2, -- source table or view
    i_whr           IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to i_src
    i_errtab        IN VARCHAR2 DEFAULT NULL, -- optional error log table,
    i_hint          IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    i_commit_at     IN NUMBER   DEFAULT 0, -- 0 - do not commit, otherwise commit
    i_uk_col_list   IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use in MERGE operation instead of PK columns
    o_add_cnt       IN OUT PLS_INTEGER, -- number of added/changed rows
    o_err_cnt       IN OUT PLS_INTEGER  -- number of errors
  ) IS
    l_operation         VARCHAR2(8);
    l_src_schema  VARCHAR2(30);
    l_src_tname   VARCHAR2(30);
    l_src_db      VARCHAR2(30);
    l_tgt_schema  VARCHAR2(30);
    l_tgt_tname   VARCHAR2(30);
    l_tgt_db      VARCHAR2(30);
    l_err_schema  VARCHAR2(30);
    l_err_tname   VARCHAR2(30);
    l_err_db      VARCHAR2(30);
    l_pk_cols     VARCHAR2(200);
    l_on_list     VARCHAR2(500);
    l_ins_cols    VARCHAR2(20000);
    l_upd_cols    VARCHAR2(20000);
    l_hint1       VARCHAR2(500);
    l_hint2       VARCHAR2(500);
    l_cmd         VARCHAR2(32000);
    l_cnt         PLS_INTEGER;
    l_tag         VARCHAR2(30);
    l_ts          TIMESTAMP;
    l_act         VARCHAR2(256);
    
    -- Procedure COLLECT_METADATA gathers information about the columns
    -- of the target table and the source table/view.
    -- Gatherd information is stored in TMP_COLUMN_INFO and
    -- then used in dynamic DML statement generation.
    PROCEDURE collect_metadata IS
      PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
      l_cmd := 'INSERT INTO tmp_all_columns(side, owner, table_name, column_id, column_name, data_type, uk, nullable)
      SELECT ''SRC'', cl.owner, cl.table_name, cl.column_id, cl.column_name, cl.data_type, ''N'', ''Y''
      FROM all_tab_columns'||l_src_db||' cl
      WHERE cl.owner = '''||l_src_schema||''' AND cl.table_name = '''||l_src_tname||'''
      UNION
      SELECT ''TGT'', cl.owner, cl.table_name, cl.column_id, cl.column_name, cl.data_type, NVL2(cc.column_name, ''Y'', ''N'') uk, cl.nullable
      FROM all_tab_columns cl' || 
      CASE WHEN i_uk_col_list IS NOT NULL THEN '
      LEFT JOIN 
      (
        SELECT VALUE(t) column_name 
        FROM TABLE(fnc_split_string(UPPER('''||i_uk_col_list||'''))) t
      ) cc ON cc.column_name = cl.column_name' 
      ELSE '
      LEFT JOIN all_constraints c
        ON c.owner = cl.owner AND c.table_name = cl.table_name
       AND c.constraint_type = ''P''
      LEFT JOIN all_cons_columns cc
        ON cc.owner = c.owner
       AND cc.constraint_name = c.constraint_name
       AND cc.column_name = cl.column_name ' 
      END || '
      WHERE cl.owner = '''||l_tgt_schema||''' AND cl.table_name = '''||l_tgt_tname||'''';
      
      EXECUTE IMMEDIATE l_cmd;
      l_cmd := NULL;
      
      SELECT
        fnc_concat_v2_set(CURSOR(
          SELECT column_name
          FROM tmp_all_columns
          WHERE side = 'TGT' AND uk = 'Y'
          ORDER BY column_id)
        ),
        fnc_concat_v2_set(CURSOR(
          SELECT 
            CASE nullable 
              WHEN 'N' THEN 't.'||column_name||'=q.'||column_name
              ELSE CASE
                WHEN data_type LIKE '%CHAR%' THEN
                  'NVL(t.'||column_name||', ''$$N/A$$'') = NVL(q.'||column_name||', ''$$N/A$$'')'
                WHEN data_type = 'DATE' THEN
                  'NVL(t.'||column_name||', DATE ''0001-01-01'') = NVL(q.'||column_name||', DATE ''0001-01-01'')'
                WHEN data_type LIKE 'TIME%' THEN
                  'NVL(t.'||column_name||', TIMESTAMP ''0001-01-01 00:00:00'') = NVL(q.'||column_name||', TIMESTAMP ''0001-01-01 00:00:00'')'
                ELSE 'NVL(t.'||column_name||', -101010101) = NVL(q.'||column_name||', -101010101)'
              END
            END
          FROM tmp_all_columns
          WHERE side = 'TGT' AND uk = 'Y'
          ORDER BY column_id), ' AND '
        ),
        fnc_concat_v2_set(CURSOR(
          SELECT 't.'||column_name||'=q.'||column_name
          FROM tmp_all_columns WHERE side = 'TGT'
          INTERSECT
          SELECT 't.'||column_name||'=q.'||column_name
          FROM tmp_all_columns WHERE side = 'SRC'
          MINUS
          SELECT 't.'||column_name||'=q.'||column_name
          FROM tmp_all_columns WHERE side = 'TGT' AND uk = 'Y')
        ),
        fnc_concat_v2_set(CURSOR(
          SELECT 'q.'||tc.column_name
          FROM tmp_all_columns tc
          JOIN tmp_all_columns sc ON sc.column_name = tc.column_name
          WHERE tc.side = 'TGT' AND sc.side = 'SRC'
          ORDER BY tc.column_id)
        )
      INTO l_pk_cols, l_on_list, l_upd_cols, l_ins_cols
      FROM dual;

      COMMIT; -- to delete rows from TMP_ALL_COLUMNS
    END;
  BEGIN
    xl.begin_action('Adding data: '||i_src||' -> '||i_tgt);
    
    l_cnt := INSTR(i_operation, ' ');
    IF l_cnt = 0 THEN l_cnt := LENGTH(i_operation); END IF;
    
    l_operation := RTRIM(UPPER(SUBSTR(i_operation, 1, l_cnt)));
    
    l_hint1 := SUBSTR(i_operation, l_cnt+1);

    IF l_operation = 'APPEND' THEN
      l_operation := 'INSERT'; l_hint1 := '/*+ APPEND */';
    END IF;

    IF l_operation NOT IN ('INSERT', 'UPDATE', 'MERGE', 'REPLACE', 'EQUALIZE') THEN
      Raise_Application_Error(-20000, 'Unsupported operation: '||l_operation);
    END IF;

    IF i_hint IS NOT NULL THEN
      l_hint2 := '/*+ '||i_hint||' */';
    END IF;
    
    l_tag := NVL(TO_CHAR(g_proc_id), SYS_CONTEXT('USERENV','SESSIONID'));
    
    resolve_name(i_src, l_src_schema, l_src_tname, l_src_db);
    resolve_name(i_tgt, l_tgt_schema, l_tgt_tname, l_tgt_db);
    IF i_errtab IS NOT NULL THEN
      resolve_name(i_errtab, l_err_schema, l_err_tname, l_err_db);
    END IF;
    
    collect_metadata;
    
    IF l_ins_cols IS NULL THEN
      Raise_Application_Error
      (
        -20000,
        'No common columns found for the source and target tables. '||
        'Check that you have access to both of them and they have matching columns.'
      );
    END IF;
    
    IF l_operation IN ('UPDATE', 'MERGE', 'EQUALIZE') AND l_pk_cols IS NULL THEN -- OK-130808
      Raise_Application_Error(-20000, 'No Key specified for '||l_tgt_schema||'.'||l_tgt_tname);
    END IF;
    
    l_ts := SYSTIMESTAMP;

    IF l_operation = 'REPLACE' THEN
      xl.begin_action('Truncating table '||i_tgt);
        EXECUTE IMMEDIATE 'TRUNCATE TABLE '||i_tgt;
        l_operation := 'APPEND';
      xl.end_action;

    ELSIF l_operation = 'EQUALIZE' THEN
      xl.begin_action('Deleting extra data from '||i_tgt);
        l_cmd := '
        DELETE FROM '||i_tgt||'
        WHERE ('||l_pk_cols||') NOT IN
        (
          SELECT '||l_pk_cols||'
          FROM '||i_src||' '||i_whr||'
        )';
        EXECUTE IMMEDIATE l_cmd;
        l_cnt := SQL%ROWCOUNT;
        l_operation := 'MERGE';
      xl.end_action(l_cnt||' rows deleted');
    END IF;
    
    IF l_operation = 'INSERT' AND i_commit_at > 0 THEN -- incremental insert with commit afrer each portion
      l_cmd := '
      DECLARE
        CURSOR cur IS
        SELECT '||l_hint2||' '||REPLACE(LOWER(l_ins_cols), 'q.')||' FROM '||i_src||' q '||i_whr||';
          
        TYPE buffer_type IS TABLE OF cur%ROWTYPE;
          
        bfr  buffer_type;
        cnt  PLS_INTEGER;
      BEGIN
        :add_cnt := 0;
        
        OPEN cur;
        LOOP
          FETCH cur BULK COLLECT INTO bfr LIMIT :commit_at;
          cnt := bfr.COUNT;

          FORALL i IN 1..cnt
          INSERT INTO '||i_tgt||'('||REPLACE(LOWER(l_ins_cols), 'q.')||')
          VALUES('||REPLACE(LOWER(l_ins_cols), 'q.', 'bfr(i).')||')'||
          CASE WHEN l_err_tname IS NOT NULL THEN '
          LOG ERRORS INTO '||l_err_schema||'.'||l_err_tname||' (:tag) REJECT LIMIT UNLIMITED' END||';

          :add_cnt := :add_cnt + SQL%ROWCOUNT;
          COMMIT;

          IF cnt < :commit_at THEN
            EXIT;
          END IF;

          xl.end_action(:add_cnt||'' rows inserted so far'');
          xl.begin_action(:act, ''Continue ...'');
        END LOOP;
        CLOSE cur;
      END;';
      
      l_act := 'Inserting rows by portions';
      xl.begin_action(l_act, l_cmd);
        IF l_err_tname IS NOT NULL THEN
          EXECUTE IMMEDIATE l_cmd USING IN OUT o_add_cnt, i_commit_at, l_tag, l_act;
        ELSE
          EXECUTE IMMEDIATE l_cmd USING IN OUT o_add_cnt, i_commit_at, l_act;
        END IF;
        l_cmd := NULL;
      xl.end_action('Totally inserted: '||o_add_cnt||' rows');

    ELSE -- "one-shot" load with or without commit
      l_cmd := CASE WHEN l_operation IN ('UPDATE', 'MERGE') THEN '
      MERGE '||l_hint1||' INTO '||i_tgt||' t USING
      (
        SELECT '||l_hint2||' *
        FROM '||i_src||' '||i_whr||'
      ) q
      ON ('||l_on_list||')'||
      CASE WHEN l_upd_cols IS NOT NULL THEN '
      WHEN MATCHED THEN UPDATE SET '||l_upd_cols
      END ||
      CASE WHEN l_operation = 'MERGE' THEN '
      WHEN NOT MATCHED THEN INSERT ('||REPLACE(l_ins_cols, 'q.')||') VALUES ('||l_ins_cols||')' END
      ELSE '
      INSERT '||l_hint1||'
      INTO '||i_tgt||'('||REPLACE(l_ins_cols, 'q.')||')
      SELECT '||l_hint2||' '||l_ins_cols||' FROM '||i_src||' q '||i_whr
      END || CASE WHEN l_err_tname IS NOT NULL THEN '
      LOG ERRORS INTO '||l_err_schema||'.'||l_err_tname||' (:tag) REJECT LIMIT UNLIMITED' END;
      
      xl.begin_action('Executing command', l_cmd);
        IF l_err_tname IS NOT NULL THEN
          EXECUTE IMMEDIATE l_cmd USING l_tag;
          o_add_cnt := SQL%ROWCOUNT;
        ELSE
          EXECUTE IMMEDIATE l_cmd;
          o_add_cnt := SQL%ROWCOUNT;
        END IF;
        l_cmd := NULL;
      xl.end_action;
      
      IF i_commit_at <> 0 THEN
        COMMIT;
      END IF;
    END IF;

    IF l_err_tname IS NOT NULL THEN
      EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM '||l_err_schema||'.'||l_err_tname||' WHERE ora_err_tag$ = :tag AND entry_ts >= :ts'
      INTO o_err_cnt USING l_tag, l_ts;
    ELSE
      o_err_cnt := 0;
    END IF;
    
    xl.end_action(o_add_cnt||' rows added; '||o_err_cnt||' errors found');
  EXCEPTION
   WHEN OTHERS THEN
    IF l_cmd IS NOT NULL THEN
      write_log('Error while executing SQL', l_cmd, SYSTIMESTAMP, SYSTIMESTAMP);
    END IF;
    xl.end_action(SQLERRM);
    
    RAISE;
  END;

  -- "Silent" version - with no OUT parameters
  PROCEDURE add_data
  (
    i_operation   IN VARCHAR2, -- 'MERGE', 'APPEND', 'INSERT' 'REPLACE' or 'EQUALIZE'
    i_tgt         IN VARCHAR2, -- target table to add rows to
    i_src         IN VARCHAR2, -- source table/view that contains the list of rows to delete or to preserve
    i_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to i_src
    i_errtab      IN VARCHAR2 DEFAULT NULL, -- optional error log table,
    i_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    i_commit_at   IN NUMBER   DEFAULT 0, -- 0 - do not commit, otherwise commit
    i_uk_col_list IN VARCHAR2 DEFAULT NULL -- optional UK column list to use in MERGE operation instead of PK columns
  ) IS
    l_add_cnt PLS_INTEGER;
    l_err_cnt PLS_INTEGER;
  BEGIN
    add_data
    (
      i_operation, i_tgt, i_src, i_whr, i_errtab, i_hint, i_commit_at,
      i_uk_col_list, l_add_cnt, l_err_cnt
    );
  END;
  
  -- Procedure DELETE_DATA deletes from the target table (I_TGT)
  -- the data that exists (I_NOT_IN='N') or not exists (I_NOT_IN='Y')
  -- in the source table/view (I_SRC)
  -- matching rows by either all the columns of the target table Primary Key (default)
  -- or by the given list of unique columns (I_UK_COL_LIST).
  PROCEDURE delete_data
  (
    i_tgt         IN VARCHAR2, -- target table to delete rows from
    i_src         IN VARCHAR2, -- list of rows to be deleted
    i_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to i_src
    i_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    i_commit_at   IN PLS_INTEGER DEFAULT 0,
    i_uk_col_list IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    i_not_in      IN VARCHAR2 DEFAULT 'N', -- if "Y' then the condition is "NOT IN"
    o_del_cnt     IN OUT PLS_INTEGER -- number of deleted rows
  ) IS
    l_pk_cols     VARCHAR2(2000);
    l_hint        VARCHAR2(500);
    l_cmd         VARCHAR2(4000);
    
  BEGIN
    xl.begin_action('Deleting data from '||i_tgt);
    
    IF i_uk_col_list IS NOT NULL THEN
      l_pk_cols := i_uk_col_list;
      
    ELSE
      l_pk_cols := get_key_col_list(i_tgt);

      IF l_pk_cols IS NULL THEN
        Raise_Application_Error(-20000,'No Pimary Key specified for '||i_tgt);
      END IF;
    END IF;
    
    IF i_hint IS NOT NULL THEN
      l_hint := '/*+ '||i_hint||' */';
    END IF;

    l_cmd := '
    DELETE FROM '||i_tgt||' WHERE ('||REPLACE(l_pk_cols, 'q.')||') '|| CASE i_not_in WHEN 'Y' THEN 'NOT ' END||'IN
    (
      SELECT '||l_hint||' '||l_pk_cols||' FROM '||i_src||' q '||i_whr|| '
    )';
    xl.begin_action('Executing command', l_cmd);
      EXECUTE IMMEDIATE l_cmd;
      o_del_cnt := SQL%ROWCOUNT;
      l_cmd := NULL;
    xl.end_action;
    
    IF i_commit_at <> 0 THEN
      COMMIT;
    END IF;

    xl.end_action(o_del_cnt||' rows deleted');
  EXCEPTION
   WHEN OTHERS THEN
    IF l_cmd IS NOT NULL THEN
      write_log('Error while executing SQL', l_cmd, SYSTIMESTAMP, SYSTIMESTAMP);
    END IF;
    xl.end_action(SQLERRM);
    RAISE;
  END;

  -- "Silent" version - i.e. with no OUT parameter
  PROCEDURE delete_data
  (
    i_tgt         IN VARCHAR2, -- target table to delete rows from
    i_src         IN VARCHAR2, -- source table/view that contains the list of rows to delete or to preserve
    i_whr         IN VARCHAR2 DEFAULT NULL, -- optional WHERE condition to apply to I_SRC
    i_hint        IN VARCHAR2 DEFAULT NULL, -- optional hint for the source query
    i_commit_at   IN PLS_INTEGER DEFAULT 0,
    i_uk_col_list IN VARCHAR2 DEFAULT NULL, -- optional UK column list to use instead of PK columns
    i_not_in      IN VARCHAR2 DEFAULT 'N'
  ) IS
    l_del_cnt PLS_INTEGER;
  BEGIN
    delete_data(i_tgt, i_src, i_whr, i_hint, i_commit_at, i_uk_col_list, i_not_in, l_del_cnt);
  END;
  
  -- Function GET_PARTITION_INFO returns table-like structure with information
  -- about all the INTERVAL partitions of the given tables
  FUNCTION get_partition_info(i_table_list IN VARCHAR2) 
  RETURN tab_partition_info PIPELINED IS
  BEGIN
    FOR r IN
    (
      SELECT table_name, partition_name, high_value
      FROM user_tab_partitions
      WHERE table_name IN 
      (
        SELECT COLUMN_VALUE
        FROM TABLE(fnc_split_string(i_table_list))
      )
      AND INTERVAL = 'YES'
    )
    LOOP
      PIPE ROW(obj_partition_info(r.table_name, r.partition_name, SUBSTR(r.high_value, 1, 20)));
    END LOOP;
  END;
  
  -- Procedure DROP_OLD_PARTITIONS drops all INTERVAL partitins from the given tables
  -- exception the most newest ones (based on HIGH VALUE)
  PROCEDURE drop_old_partitions(i_table_list IN VARCHAR2 DEFAULT NULL, i_keep_newest IN PLS_INTEGER DEFAULT 2) IS
  BEGIN
    FOR r IN
    (
      SELECT * FROM
      (
        SELECT t.*, RANK() OVER(PARTITION BY table_name ORDER BY high_value DESC) rnk 
        FROM TABLE
        (
          get_partition_info(REPLACE(UPPER(i_table_list), ' '))
        ) t
      )
      WHERE rnk > i_keep_newest
      ORDER BY DECODE(table_name, 'DBG_PROCESS_LOGS', 2, 1), table_name, high_value
    )
    LOOP
      xl.begin_action('Dropping partition '||r.table_name||'('||r.partition_name||')');
      EXECUTE IMMEDIATE 'ALTER TABLE '||r.table_name||' DROP PARTITION '||r.partition_name;
      xl.end_action;
    END LOOP;
  END;
  
  PROCEDURE truncate_table(i_table_name IN VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE '||i_table_name;
  END;
END;
/
