CREATE OR REPLACE PACKAGE BODY IQETL.Dynamic_Type2 AS
/******************************************************************************
   NAME:       Dynamic_Type2
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        7/25/2016      sk44643       1. Created this package body.
   1.0.1        8/5/2016       sk44643       2. Added the logging procedure to capture the run  log.
   1.0.2        8/8/2016       sk44643       3. Added the Error Hand Shaking code for DataStage call.
   1.0.3        8/19/2016      sk44643       4. Added the type01 logic
******************************************************************************/
V_ERROR_MSG      VARCHAR2 (2000);
V_ERROR_NUM      VARCHAR2 (300);
v_proc_schema    VARCHAR2(100) := 'SHARAN';
sd               DATE;
ed               DATE;

/* Function Is_Table_Exist - check the DDL defination of dimensions table exist or not */
FUNCTION IS_TABLE_EXIST (P_DIM_TAB_OWNER VARCHAR2, P_DIM_TABLE_NAME VARCHAR2)
      RETURN VARCHAR2
   IS
      V_SQL           VARCHAR2 (1000);
      V_EXIST_CHECK   NUMBER;
   BEGIN      
      V_SQL :=
            'SELECT NVL(COUNT(*),0) FROM ALL_TABLES AT  WHERE AT.OWNER'
         || ' = '
         || ''''
         || P_DIM_TAB_OWNER
         || ''''
         || ' AND AT.TABLE_NAME '
         || ' = '
         || ''''
         || P_DIM_TABLE_NAME
         || '''';

    EXECUTE IMMEDIATE V_SQL INTO V_EXIST_CHECK;

      IF V_EXIST_CHECK > 0
      THEN
         RETURN ('Y');
      ELSE
         RETURN ('N');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN ('N');
   END IS_TABLE_EXIST;
   
 Procedure Load_info_logging (p_Job_name varchar2,p_table_name varchar2,p_log_msg varchar2)
    as 
        PRAGMA AUTONOMOUS_TRANSACTION;
        lv_msg VARCHAR2(4000);
        Procedure_name      VARCHAR2(100);
        v_error_msg         VARCHAR2 (2000);
        v_error_num         VARCHAR2 (300);
         
    Begin 
            Procedure_name := 'Load_info_logging';

           update ETL_JOB_RUN 
                  set LOG_MESSAGE = substr(LOG_MESSAGE||substr(p_log_msg,1,3999),1,3999)
                  where run_status = 'Running'   
                  and job_name = p_Job_Name
                  and SRC_TABLE_NAME = p_table_name;
          Commit;
    exception
              when no_data_found then rollback;
              when others then
                    v_error_msg := SUBSTR ('ERROR - ' || SQLERRM, 1, 300);
                    v_error_num := SQLCODE;
                    rollback;
                    raise;                           
end Load_info_logging;     

/* Procedure LOAD_TYPE2_PROC will dynamically create the  select clasue to pull the data from src/temp and last dimensions load */
procedure LOAD_TYPE2_PROC(P_DIM_TAB_OWNER VARCHAR2, P_DIM_SUBJECT_AREA VARCHAR2, P_DIM_TABLE_NAME VARCHAR2,P_DEBUG_MODE VARCHAR2 DEFAULT 'Y')
AS
    lv_Type2_Col_list VARCHAR2(32767);
    lv_src_tabl_owner VARCHAR2(100);
    lv_src_tabl VARCHAR2(100);
    lv_src_tbl_business_key VARCHAR2(32767);
    lv_Dim_tbl_skey VARCHAR2(100);
    
    lv_Delta_clause         VARCHAR2 (32767) := '(';
    lv_Delta_clause01         VARCHAR2 (32767) := '(';
    
    lv_BusinessKey_clause   VARCHAR2(32767):= 'WHEN (';
    lv_src_select_string VARCHAR2(32767);
    lv_src_select_string01 VARCHAR2(32767);
    lv_intrim_sql_string varchar2(32767);
    lv_intrim_sql_string01 varchar2(32767);
    lv_join_clause varchar2(32767) := '(';
    lv_utility_tbl_join varchar2(32767) := '(';
    lv_merge_join varchar2(32767) := '(';
    lv_merge_join01 varchar2(32767) := '(';

    lv_merge_update_string varchar2(32767);
    lv_merge_insert_string varchar2(32767);
    /* To handle the merge size greater then 32k */
    v_clobsql CLOB;
    v_clobsql01 CLOB;

    V_ERROR_MSG  VARCHAR2 (2000);
    V_ERROR_NUM  VARCHAR2 (300);
    V_ERROR_MSG_STRING  VARCHAR2 (32767);
    Error_logged               BOOLEAN := FALSE;
    lv_debug_flag               VARCHAR2 (1) := P_DEBUG_MODE;
    rcd_count                   NUMBER;
    --tbl_chk_sql                 varchar2(32767);
    --V_EXIST_CHECK   NUMBER;
    V_EXIST_TABLE               VARCHAR2 (1);
    ex_missing_tbl_ddl EXCEPTION;
    PRAGMA EXCEPTION_INIT( ex_missing_tbl_ddl, -20343 );
    Procedure_name VARCHAR2(1000):= 'LOAD_TYPE2_PROC';

   --type1 column list.
   lv_Type1_Col_list VARCHAR2(32767);
   lv_merge_clause01 varchar2(32767) := '(';
   lv_merge_update_string01 varchar2(32767);
   Type1Col_EXIST_chk       VARCHAR2(30) ;


Begin

  --debug flag
  IF lv_debug_flag = 'Y'
      THEN
         DBMS_OUTPUT.ENABLE (1000000);
      ELSE
         DBMS_OUTPUT.DISABLE ();
  END IF;
  
--step1  
 -- check the DDL for table exists..
  -- check is the table exists..if not error out.
  V_EXIST_TABLE := IS_TABLE_EXIST (P_DIM_TAB_OWNER, P_DIM_TABLE_NAME);
  IF V_EXIST_TABLE = 'N' THEN
       V_ERROR_MSG_STRING := 'Step_1-> DDL of Dimensions table does not exist in '||P_DIM_TAB_OWNER|| '.'|| P_DIM_TABLE_NAME;
       dbms_output.put_line(V_ERROR_MSG_STRING);
       raise ex_missing_tbl_ddl;    
  END IF;
  
--Step2
  -- Get the Type2 Columns and type 01 columns if any from the metadata table ( SCD_TYPE2_METADATA ).
  select SRC_TYPE02_Column_Name,SRC_TABLE_OWNER,SRC_TABLE_NAME,SRC_TABLE_BUSINESS_KEY,DIM_TABLE_SKEY
  into lv_Type2_Col_list,lv_src_tabl_owner,lv_src_tabl,lv_src_tbl_business_key,lv_Dim_tbl_skey from 
  SCD_TYPE2_METADATA
  where SUBJECT_AREA = P_DIM_SUBJECT_AREA AND DIM_TABLE_NAME = P_DIM_TABLE_NAME;
  --where SUBJECT_AREA = 'STUDENT' AND DIM_TABLE_NAME = 'D_DB_DEPARTMENT_TYP02';
  dbms_output.put_line(lv_Type2_Col_list||'-'||lv_src_tabl_owner||'-'||lv_src_tabl||'-'||lv_src_tbl_business_key||'-'||lv_Type1_Col_list);  
 
--Step3    
  --Start of logging  
  sd := SYSDATE;
   
  INSERT INTO ETL_JOB_RUN(JOB_OWNER,
                               JOB_NAME,
                               START_DATE,
                               END_DATE,
                               RUN_STATUS,
                               SRC_TABLE_NAME,
                               TRG_TABLE_NAME
                               )
     VALUES (v_proc_schema,
                   Procedure_name,
                   sd,
                   NULL,
                   'Running',
                   lv_src_tabl,
                   P_DIM_TABLE_NAME
                   );                                         

    COMMIT;
--step4     
  -- to get the list of columns for type 2 from the metadata table.
    if trim(upper(lv_Type2_Col_list)) = 'ALL' THEN
          dbms_output.put_line('In the ALL block');
    
    --step 4.1   
      -- get all the columns list for the table exculding the business key.
          FOR I IN ( SELECT COLUMN_NAME
                               FROM ALL_TAB_COLUMNS ATC
                              WHERE TABLE_NAME = Trim(lv_src_tabl)
                                AND OWNER = lv_src_tabl_owner
                                AND DATA_TYPE NOT IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                                AND COLUMN_NAME not in (
                                                        select regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) as val
                                                         from   SCD_TYPE2_METADATA
                                                         WHERE SRC_TABLE_NAME = Trim(lv_src_tabl)
                                                         AND SRC_TABLE_OWNER = lv_src_tabl_owner
                                                         connect by regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) is not null
                                
                                )
                                ORDER BY COLUMN_ID
                    )
          loop       
              lv_Delta_clause := lv_Delta_clause ||' decode( '||' s.'||I.COLUMN_NAME||','|| 'delta'||'.'||I.COLUMN_NAME||','||0||','||1||')'||' = '|| 1||' OR '; 
          end loop;  
          
          lv_Delta_clause := SUBSTR (lv_Delta_clause, 1, INSTR (lv_Delta_clause, 'OR', -1) - 1)|| ')'; 
          --lv_Delta_clause := SUBSTR (lv_Delta_clause, 1, INSTR (lv_Delta_clause, ' OR ', -1) - 1)|| ')';    
          dbms_output.put_line('Delta_clause'||lv_Delta_clause);
           /* 
                ( decode(  s.DEPT_LONG_TITLE,delta.DEPT_LONG_TITLE,0,1) = 1  OR  
                  decode(  s.DEPT_SHORT_TITLE,delta.DEPT_SHORT_TITLE,0,1) = 1 OR  
                  decode(  s.EXTRACT_DATE,delta.EXTRACT_DATE,0,1) = 1 
                )
           */ 
                  
        --step 4.2
          -- get the business key check list to insert the new records.
          FOR I IN ( select regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) Natural_key
                                                         from   SCD_TYPE2_METADATA
                                                         WHERE SRC_TABLE_NAME = Trim(lv_src_tabl)
                                                         AND SRC_TABLE_OWNER = lv_src_tabl_owner
                                                         connect by regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) is not null
                                                         )                                   
          loop       
              lv_BusinessKey_clause := lv_BusinessKey_clause ||'delta.'||I.Natural_key|| ' IS NULL' ||' OR ';           
              --Get the business key ON  DELTA.DEPARTMENT_CODE = s.DEPARTMENT_CODE 
              lv_join_clause := lv_join_clause||' delta.'||I.Natural_key||' = '||'s.'||I.Natural_key||' AND ';
              --type merge join
              lv_merge_clause01 := lv_merge_clause01||' mp.'||I.Natural_key||' = '||'trg.'||I.Natural_key||' AND ';
                        
          end loop;
          
          lv_BusinessKey_clause := SUBSTR (lv_BusinessKey_clause, 1, INSTR (lv_BusinessKey_clause, 'OR', -1) - 1)|| ')';       
          dbms_output.put_line('bus_key'||lv_BusinessKey_clause);

          lv_join_clause := lv_join_clause||' delta.'||'RCD_CURRENT_IND = '||'''Y'''||' )';
          dbms_output.put_line('join_key'||lv_join_clause);
  else
          dbms_output.put_line('In the else block');
      -- Get the list of type 2 columns specified in the metadata table ( SCD_TYPE2_METADATA ).
          FOR I IN ( select regexp_substr(SRC_TYPE02_Column_Name,'[^,]+',1,level) COLUMN_NAME
                                                         from   SCD_TYPE2_METADATA
                                                         WHERE SRC_TABLE_NAME = Trim(lv_src_tabl)
                                                         AND SRC_TABLE_OWNER = lv_src_tabl_owner
                                                         connect by regexp_substr(SRC_TYPE02_Column_Name,'[^,]+',1,level) is not null                        
                   )
          loop       
          lv_Delta_clause := lv_Delta_clause ||' decode( '||' s.'||I.COLUMN_NAME||','|| 'delta'||'.'||I.COLUMN_NAME||','||0||','||1||')'||' = '|| 1||' OR '; 
          end loop;  
          lv_Delta_clause := SUBSTR (lv_Delta_clause, 1, INSTR (lv_Delta_clause, 'OR', -1) - 1)|| ')'; 
          --lv_Delta_clause := SUBSTR (lv_Delta_clause, 1, INSTR (lv_Delta_clause, ' OR ', -1) - 1)|| ')';    
          dbms_output.put_line('Delta_clause'||lv_Delta_clause);
  
      -- get the business key check list to insert the new records.
         FOR I IN ( select regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) Natural_key
                                                         from   SCD_TYPE2_METADATA
                                                         WHERE SRC_TABLE_NAME = Trim(lv_src_tabl)
                                                         AND SRC_TABLE_OWNER = lv_src_tabl_owner
                                                         connect by regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) is not null
                                                         )                                   
         loop       
          lv_BusinessKey_clause := lv_BusinessKey_clause ||'delta.'||I.Natural_key|| ' IS NULL' ||' OR ';          
         --Get the business key ON  DELTA.DEPARTMENT_CODE = s.DEPARTMENT_CODE 
          lv_join_clause := lv_join_clause||' delta.'||I.Natural_key||' = '||'s.'||I.Natural_key||' AND ';
          --type merge join
          lv_merge_clause01 := lv_merge_clause01||' mp.'||I.Natural_key||' = '||'trg.'||I.Natural_key||' AND ';
           
          end loop;  
          lv_BusinessKey_clause := SUBSTR (lv_BusinessKey_clause, 1, INSTR (lv_BusinessKey_clause, 'OR', -1) - 1)|| ')';
          dbms_output.put_line('bus_key'||lv_BusinessKey_clause);
         
          lv_join_clause := lv_join_clause||' delta.'||'RCD_CURRENT_IND = '||'''Y'''||' )';
          dbms_output.put_line('join_key'||lv_join_clause);

  end if;
  
--step 5
   -- build the select statement for the src table columns.
  lv_intrim_sql_string := 'SELECT '||CHR(10);
  lv_src_select_string := 'SELECT '||CHR(10);      
   FOR I IN ( SELECT COLUMN_NAME
                               FROM ALL_TAB_COLUMNS ATC
                              WHERE TABLE_NAME = Trim(lv_src_tabl)
                                AND OWNER = lv_src_tabl_owner
                                AND DATA_TYPE NOT IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                                ORDER BY COLUMN_ID
                             )
     loop
            lv_intrim_sql_string := lv_intrim_sql_string ||' s.'|| I.COLUMN_NAME || ',';
            lv_src_select_string := lv_src_select_string ||' m.'|| I.COLUMN_NAME || ',';
     end loop;
     -- Add the surrogate key columns and comparasion clause created above.
     lv_intrim_sql_string := lv_intrim_sql_string||' delta.'||lv_Dim_tbl_skey||chr(10); 
     lv_intrim_sql_string := lv_intrim_sql_string||chr(10)|| ','||'CASE  '||lv_BusinessKey_clause||' THEN 1'||' WHEN '||lv_Delta_clause || ' THEN 2 '||chr(10)||' ELSE 0 ' ||chr(10)||' end scd_row_type_id';
     lv_intrim_sql_string := lv_intrim_sql_string||chr(10)||'FROM '||lv_src_tabl_owner||'.'||lv_src_tabl||' s '||chr(10);
     lv_intrim_sql_string := lv_intrim_sql_string||chr(10)||'LEFT OUTER JOIN '||P_DIM_TAB_OWNER||'.'||P_DIM_TABLE_NAME||' delta '||chr(10);
     lv_intrim_sql_string := lv_intrim_sql_string||' ON '||lv_join_clause||chr(10);
          
     dbms_output.put_line('intrim - '||lv_intrim_sql_string);
          
     -- build the outer query to get the surrogate key keys.
     -- Used the negative number for the surrogate key as we have to use only the positive number for surrogate key in dimension table.
     -- For the utility match table ( 1 - Insert , 2 - update ) 
        lv_src_select_string := lv_src_select_string||chr(10)||'m.SCD_ROW_TYPE_ID'||' ,'||chr(10)||'Decode(s.scd_row_type_id,1,-6789'||','||' m.'||lv_Dim_tbl_skey||') '|| lv_Dim_tbl_skey;
        dbms_output.put_line('src - '||lv_src_select_string);
          
     -- Get the join condition of data with utility table SCD_ROW_TYPE.
        lv_utility_tbl_join := lv_utility_tbl_join||' s.scd_row_type_id <= m.scd_row_type_id'||')';
        dbms_output.put_line('utility join - '||lv_utility_tbl_join);
                
     -- Merge Join condition.
        lv_merge_join := lv_merge_join||'mp.'||lv_Dim_tbl_skey||' = '||'trg.'||lv_Dim_tbl_skey||' )';
        dbms_output.put_line('merge join - '||lv_merge_join);
       
     -- Build the merge insert.
        lv_merge_insert_string := '(';
      --1) get the target columns list.
       FOR I IN ( SELECT COLUMN_NAME
                               FROM ALL_TAB_COLUMNS ATC
                               WHERE TABLE_NAME = Trim(P_DIM_TABLE_NAME)
                               AND OWNER = P_DIM_TAB_OWNER
                               AND DATA_TYPE NOT IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                               AND COLUMN_NAME not in ('RCD_CURRENT_IND','RCD_EFFECTIVE_TS','RCD_EXPIRATION_TS')
                               AND COLUMN_NAME not in (
                                                        select regexp_substr(DIM_TABLE_SKEY,'[^,]+',1,level) as val
                                                         from   SCD_TYPE2_METADATA
                                                         WHERE SRC_TABLE_NAME = Trim(lv_src_tabl)
                                                         AND SRC_TABLE_OWNER = lv_src_tabl_owner
                                                         connect by regexp_substr(DIM_TABLE_SKEY,'[^,]+',1,level) is not null
                                                      )
                               
                               ORDER BY COLUMN_ID
                )
        loop        
        --if I.COLUMN_NAME <> 'RCD_CURRENT_IND' OR I.COLUMN_NAME <> 'RCD_EFFECTIVE_TS' OR I.COLUMN_NAME <> 'RCD_EXPIRATION_TS' THEN              
          lv_merge_insert_string := lv_merge_insert_string || 'Trg.' || I.COLUMN_NAME || ',';
        --end if;
        end loop;        
          lv_merge_insert_string := SUBSTR (lv_merge_insert_string, 1, INSTR (lv_merge_insert_string, ',', -1) - 1) || ')';
          lv_merge_insert_string := lv_merge_insert_string || 'VALUES(' || CHR (10); 
          
          dbms_output.put_line('merge insert join - '||lv_merge_insert_string); 
               
       --2) Get the value clause for insert.
         FOR I IN ( SELECT COLUMN_NAME
                               FROM ALL_TAB_COLUMNS ATC
                               WHERE TABLE_NAME = Trim(P_DIM_TABLE_NAME)
                               AND OWNER = P_DIM_TAB_OWNER
                               AND DATA_TYPE NOT IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                               AND COLUMN_NAME not in ('RCD_CURRENT_IND','RCD_EFFECTIVE_TS','RCD_EXPIRATION_TS')
                               AND COLUMN_NAME not in (
                                                        select regexp_substr(DIM_TABLE_SKEY,'[^,]+',1,level) as val
                                                         from   SCD_TYPE2_METADATA
                                                         WHERE SRC_TABLE_NAME = Trim(lv_src_tabl)
                                                         AND SRC_TABLE_OWNER = lv_src_tabl_owner
                                                         connect by regexp_substr(DIM_TABLE_SKEY,'[^,]+',1,level) is not null
                                                      )
                               ORDER BY COLUMN_ID
                   )
        loop  
        --case when I.COLUMN_NAME not in ( 'RCD_CURRENT_IND','RCD_EFFECTIVE_TS', 'RCD_EXPIRATION_TS' ) THEN             
        lv_merge_insert_string := lv_merge_insert_string || 'mp.' || I.COLUMN_NAME || ',';
        --ELSE NULL; 
        --END case;             
        --end if;             
        end loop;        
         lv_merge_insert_string := SUBSTR (lv_merge_insert_string, 1, INSTR (lv_merge_insert_string, ',', -1) - 1) || ')';
         dbms_output.put_line('merge insert join1 - '||lv_merge_insert_string); 
     
     -- build the merge update.
         --lv_merge_update_string := '(';
         lv_merge_update_string := lv_merge_update_string||' '||'trg.'||'RCD_CURRENT_IND'||' = ' || '''N''' || ','||chr(10);
         --lv_merge_update_string := lv_merge_update_string||' '||'trg.'||'RCD_EXPIRATION_TS'||' = ' || 'sysdate-5/1440' ||chr(10);
         lv_merge_update_string := lv_merge_update_string||' '||'trg.'||'RCD_EXPIRATION_TS'||' = ' || 'sysdate' ||chr(10);          
         dbms_output.put_line('merge update string - '||lv_merge_update_string);  

--Step 6
     --Step Build the final sql statement.            
              v_clobsql := 'MERGE /*+ APPEND PARALLEL (MANUAL) LOAD_TYPE2_PROC '||P_DIM_TAB_OWNER||'.'||P_DIM_TABLE_NAME||' */ 
                            INTO '||P_DIM_TAB_OWNER||'.'||P_DIM_TABLE_NAME||' Trg USING ('||Chr(10);              
              v_clobsql := v_clobsql||chr(10)||lv_src_select_string||chr(10)||'FROM ('||chr(10)||lv_intrim_sql_string||')'||'m'||chr(10); 
              v_clobsql := v_clobsql||' INNER JOIN '|| 'SCD_ROW_TYPE S '||chr(10)|| ' ON ' ||lv_utility_tbl_join|| chr(10)||')'||' mp '||chr(10);
              v_clobsql := v_clobsql||chr(10)||' ON '||lv_merge_join;
              v_clobsql := v_clobsql||chr(10)||'WHEN MATCHED THEN UPDATE SET'||chr(10)||lv_merge_update_string||chr(10)||
                          'WHEN NOT MATCHED THEN INSERT'||chr(10)||lv_merge_insert_string;
     -- print the string and log it in the etl_job_run table in case debug mode is set to 'Y'     
        DBMS_output.put_line ('Build Merge SQL Code length '||length(v_clobsql));
        DBMS_output.put_line ('Build Merge SQL Code length '||Chr(10)||substr(v_clobsql,1,32000));
        DBMS_output.put_line (substr(v_clobsql,32001,64000));
        DBMS_output.put_line (substr(v_clobsql,64001,96000));   
        
     -- execute the sql statement
        EXECUTE IMMEDIATE v_clobsql;
        rcd_count := sql%rowcount;
        commit;
             
        DBMS_output.put_line ('Counts of Merge rows := '||rcd_count);

---******************************************** type 01 logic***********************************************
  -- check to see if there is any columns to process as type 1
      SELECT NVL (COUNT (COLUMN_NAME), 0) INTO Type1Col_EXIST_chk
                        FROM ALL_TAB_COLUMNS ATC
                        WHERE TABLE_NAME     = Trim(lv_src_tabl)
                        AND OWNER            = lv_src_tabl_owner
                        AND DATA_TYPE NOT   IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                        AND COLUMN_NAME NOT IN
                          (SELECT regexp_substr(SRC_TYPE02_COLUMN_NAME,'[^,]+',1,level) AS val
                          FROM SCD_TYPE2_METADATA
                          WHERE SRC_TABLE_NAME                                                = Trim(lv_src_tabl)
                          AND SRC_TABLE_OWNER                                                 = lv_src_tabl_owner
                            CONNECT BY regexp_substr(SRC_TYPE02_COLUMN_NAME,'[^,]+',1,level) IS NOT NULL
                          )
                        AND COLUMN_NAME NOT IN
                          (SELECT regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) AS val
                          FROM SCD_TYPE2_METADATA
                          WHERE SRC_TABLE_NAME                                                = Trim(lv_src_tabl)
                          AND SRC_TABLE_OWNER                                                 = lv_src_tabl_owner
                            CONNECT BY regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) IS NOT NULL
                          )
                        ORDER BY COLUMN_ID;
                        
     if trim(upper(lv_Type2_Col_list)) <> 'ALL' AND Type1Col_EXIST_chk > 0 THEN
          dbms_output.put_line('In the ALL block');
         
        -- build the select statement for the src table columns.
        lv_intrim_sql_string01 := 'SELECT '||CHR(10);
        lv_src_select_string01 := 'SELECT '||CHR(10); 
                   
     -- step Get the comparasion columns
     -- get all the columns list for the table exculding the business key.
        FOR I IN ( SELECT COLUMN_NAME
                        FROM ALL_TAB_COLUMNS ATC
                        WHERE TABLE_NAME     = Trim(lv_src_tabl)
                        AND OWNER            = lv_src_tabl_owner
                        AND DATA_TYPE NOT   IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                        AND COLUMN_NAME NOT IN
                          (SELECT regexp_substr(SRC_TYPE02_COLUMN_NAME,'[^,]+',1,level) AS val
                          FROM SCD_TYPE2_METADATA
                          WHERE SRC_TABLE_NAME                                                = Trim(lv_src_tabl)
                          AND SRC_TABLE_OWNER                                                 = lv_src_tabl_owner
                            CONNECT BY regexp_substr(SRC_TYPE02_COLUMN_NAME,'[^,]+',1,level) IS NOT NULL
                          )
                        AND COLUMN_NAME NOT IN
                          (SELECT regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) AS val
                          FROM SCD_TYPE2_METADATA
                          WHERE SRC_TABLE_NAME                                                = Trim(lv_src_tabl)
                          AND SRC_TABLE_OWNER                                                 = lv_src_tabl_owner
                            CONNECT BY regexp_substr(SRC_TABLE_BUSINESS_KEY,'[^,]+',1,level) IS NOT NULL
                          )
                        ORDER BY COLUMN_ID                       
                   )
          loop 
           -- make the comparison string columns.      
          lv_Delta_clause01 := lv_Delta_clause01 ||' decode( '||' s.'||I.COLUMN_NAME||','|| 'delta'||'.'||I.COLUMN_NAME||','||0||','||1||')'||' = '|| 1||' OR ';                    
          -- make the update string columns.
          lv_merge_update_string01 := lv_merge_update_string01 || 'trg.' || I.COLUMN_NAME || ' = ' || 'mp.'|| I.COLUMN_NAME|| ',';          
          end loop;  
          --remove the last OR in the String
          lv_Delta_clause01 := SUBSTR (lv_Delta_clause01, 1, INSTR (lv_Delta_clause01, 'OR', -1) - 1)|| ')';  
          dbms_output.put_line('Delta_clause01'||lv_Delta_clause01);          
         --remove the last "," in the String
          lv_merge_update_string01 := SUBSTR (lv_merge_update_string01, 1, INSTR (lv_merge_update_string01, ',', -1) - 1);      
          dbms_output.put_line( 'The update string for type 01 -'|| lv_merge_update_string01);
      
      -- Get the select columns    
      FOR I IN ( SELECT COLUMN_NAME
                               FROM ALL_TAB_COLUMNS ATC
                              WHERE TABLE_NAME = Trim(lv_src_tabl)
                                AND OWNER = lv_src_tabl_owner
                                AND DATA_TYPE NOT IN ('BLOB','CLOB','RAW','SDO_GEOMETRY','LONG')
                                ORDER BY COLUMN_ID
               )
     loop
            lv_intrim_sql_string01 := lv_intrim_sql_string01 ||' s.'|| I.COLUMN_NAME || ',';
            lv_src_select_string01 := lv_src_select_string01 ||' m.'|| I.COLUMN_NAME || ',';
     end loop;
     --remove the comma at the end of lv_src_select_string01   
      lv_src_select_string01 := SUBSTR (lv_src_select_string01, 1, INSTR (lv_src_select_string01, ',', -1) - 1);                    
     -- Add the surrogate key columns and comparasion clause created above.
     lv_intrim_sql_string01 := lv_intrim_sql_string01||' delta.'||lv_Dim_tbl_skey||chr(10); 
     lv_intrim_sql_string01 := lv_intrim_sql_string01||chr(10)|| ','||'CASE  '||lv_BusinessKey_clause||' THEN 1'||' WHEN '||lv_Delta_clause01 || ' THEN 2 '||chr(10)||' ELSE 0 ' ||chr(10)||' end scd_row_type_id';
     lv_intrim_sql_string01 := lv_intrim_sql_string01||chr(10)||'FROM '||lv_src_tabl_owner||'.'||lv_src_tabl||' s '||chr(10);
     lv_intrim_sql_string01 := lv_intrim_sql_string01||chr(10)||'LEFT OUTER JOIN '||P_DIM_TAB_OWNER||'.'||P_DIM_TABLE_NAME||' delta '||chr(10);
     lv_intrim_sql_string01 := lv_intrim_sql_string01||' ON '||lv_join_clause||chr(10);
          
     dbms_output.put_line('intrim 01 for type 1- '||lv_intrim_sql_string01);     
     -- Add the string to pull the Current record only.
     lv_merge_clause01 := lv_merge_clause01||' '||'trg.'||'RCD_CURRENT_IND'||' = ' || '''Y''' || ')'||chr(10);
     dbms_output.put_line('merge join for type 01 - '||lv_merge_clause01); 
                    
  -- build the sql statement..
      --Step Build the final sql statement for type 01           
              v_clobsql01 := 'MERGE /*+ APPEND PARALLEL (MANUAL) LOAD_TYPE2_PROC '||P_DIM_TAB_OWNER||'.'||P_DIM_TABLE_NAME||' */ 
                            INTO '||P_DIM_TAB_OWNER||'.'||P_DIM_TABLE_NAME||' Trg USING ('||Chr(10);              
              v_clobsql01 := v_clobsql01||chr(10)||lv_src_select_string01||chr(10)||'FROM ('||chr(10)||lv_intrim_sql_string01||')'||'m'||chr(10);               
              --v_clobsql01 := v_clobsql01||' INNER JOIN '|| 'SCD_ROW_TYPE S '||chr(10)|| ' ON ' ||lv_utility_tbl_join|| chr(10)||')'||' mp '||chr(10);
              v_clobsql01 := v_clobsql01||' WHERE '||'m.'||'SCD_ROW_TYPE_ID '||' = '||2||chr(10)||')'||' mp '||chr(10);              
              v_clobsql01 := v_clobsql01||chr(10)||' ON '||lv_merge_clause01;
              v_clobsql01 := v_clobsql01||chr(10)||'WHEN MATCHED THEN UPDATE SET'||chr(10)||lv_merge_update_string01||chr(10);
                          --'WHEN NOT MATCHED THEN INSERT'||chr(10)||lv_merge_insert_string;                          
     -- print the string and log it in the etl_job_run table in case debug mode is set to 'Y'     
        DBMS_output.put_line ('Build Merge SQL Code length type 01 '||length(v_clobsql01));
        DBMS_output.put_line ('Build Merge SQL Code length type 01 '||Chr(10)||substr(v_clobsql01,1,32000));
        DBMS_output.put_line (substr(v_clobsql01,32001,64000));
        DBMS_output.put_line (substr(v_clobsql01,64001,96000)); 
  
        --execute the sql statement
        EXECUTE IMMEDIATE v_clobsql01;
        rcd_count := sql%rowcount;
        commit;             
        DBMS_output.put_line ('Counts of Merge rows := '||rcd_count);       
   else
        DBMS_output.put_line ('All the Columns specified is Type 2 columns');           
   end if;
      
--**************** type 01 update ends-------------------
        if lv_debug_flag = 'Y' then                        
        update ETL_JOB_RUN  set SQL_TEXT = v_clobsql||chr(10)||' Type_01_SQL '||chr(10)||v_clobsql01 WHERE SRC_TABLE_NAME  = lv_src_tabl AND  TRG_TABLE_NAME =   P_DIM_TABLE_NAME AND run_status = 'Running';                                     
        Commit;
        end if;
        
        --Update the etl_job_run table to mark the completion.
        ed := sysdate;
        UPDATE ETL_JOB_RUN set end_date=ed, run_status='Completed', run_time_seconds=round(((ed-sd)*86400),4), run_time_minutes=round((ed-sd)*(86400/60),4), rcd_processed=rcd_count
                  where run_status = 'Running'
                  and job_name = Procedure_name
                  and SRC_TABLE_NAME  = lv_src_tabl AND  TRG_TABLE_NAME =   P_DIM_TABLE_NAME;
        Commit;
      
 Exception
    WHEN ex_missing_tbl_ddl then   
    DBMS_STANDARD.raise_application_error(-20343, 'Dimensions table does not exists',TRUE);  
        
     WHEN OTHERS THEN 
            V_ERROR_MSG := SUBSTR ('ERROR - ' || SQLERRM, 1, 300);
            V_ERROR_NUM := SQLCODE;
            V_ERROR_MSG_STRING := V_ERROR_NUM||Chr(10)|| v_error_msg||Chr(10);
            Error_logged := true;
      If Error_logged then
            --email?? this will be handled by the Stonebranch scheduler.
            DBMS_output.put_line ('Capturing the error');           
            Load_info_logging(Procedure_name,lv_src_tabl,V_ERROR_MSG_STRING);
            ed := sysdate;
            update ETL_JOB_RUN set end_date=ed, run_status='Error', run_time_seconds=round(((ed-sd)*86400),2), run_time_minutes=round((ed-sd)*(86400/60),2), RCD_PROCESSED=-2,
            SQL_TEXT = v_clobsql||chr(10)||' Type_01_SQL '||chr(10)||v_clobsql01
            where run_status = 'Running'
            and job_name = Procedure_name
            and SRC_TABLE_NAME  = lv_src_tabl AND  TRG_TABLE_NAME =   P_DIM_TABLE_NAME;        
            commit;                        
        end if;      
end LOAD_TYPE2_PROC;

END Dynamic_Type2;
/