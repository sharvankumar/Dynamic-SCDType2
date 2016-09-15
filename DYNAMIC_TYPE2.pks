CREATE OR REPLACE PACKAGE IQETL.Dynamic_Type2 AS
/******************************************************************************
   NAME:       Dynamic_Type2
   PURPOSE:
   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        7/25/2016      sk44643       1. Created this package.
******************************************************************************/

 /* Utility function for logging the run into the ETL_JOB_RUN Table */
 PROCEDURE LOAD_INFO_LOGGING (P_JOB_NAME VARCHAR2,P_TABLE_NAME VARCHAR2,P_LOG_MSG VARCHAR2);
 PROCEDURE LOAD_TYPE2_PROC(P_DIM_TAB_OWNER VARCHAR2, P_DIM_SUBJECT_AREA VARCHAR2, P_DIM_TABLE_NAME VARCHAR2,P_DEBUG_MODE VARCHAR2 DEFAULT 'Y');

END Dynamic_Type2;
/