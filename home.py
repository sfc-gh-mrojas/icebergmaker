import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, contains
import os
import json
from snowflake.snowpark.context import get_active_session
import time


TASK_SQL="""CREATE OR REPLACE TASK ICEBERG_MAKER_TASK
   USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = XSMALL
    SCHEDULE = '1 MINUTE'
  AS
--EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR SELECT * FROM __external_tables_info ;
BEGIN
  OPEN c1;
  LET TABLE_CATALOG VARCHAR := '';
  LET TABLE_SCHEMA  VARCHAR := '';
  LET TABLE_NAME    VARCHAR := '';
  LET TABLE_FORMAT  VARCHAR := '';
  LET MSG VARCHAR := '';
  FOR r IN c1 DO
    BEGIN
        TABLE_CATALOG := r.TABLE_CATALOG;
        TABLE_SCHEMA  := r.TABLE_SCHEMA;
        TABLE_NAME    := r.TABLE_NAME;
        TABLE_FORMAT  := r.TABLE_FORMAT;
        LET FULLNAME VARCHAR := (:TABLE_CATALOG || '.' || :TABLE_SCHEMA || '.' || :TABLE_NAME);
        LET STAGE VARCHAR := r.STAGE;
        STAGE := SUBSTRING(:STAGE,2);
        DESCRIBE STAGE IDENTIFIER(:STAGE);
        LET query_id_stage := SQLID;
        LET URL VARCHAR := (select PARSE_JSON("property_value")[0]::STRING from table(result_scan(:query_id_stage)) where "property" = 'URL');
        LET object_priviledges VARCHAR := :TABLE_CATALOG || '.INFORMATION_SCHEMA.OBJECT_PRIVILEGES';
        LET columns_table      VARCHAR := :TABLE_CATALOG || '.INFORMATION_SCHEMA.COLUMNS';

        LET INVALID_COLUMNS ARRAY := (SELECT array_agg(column_name || ' (' || data_type || ')') as cols
                FROM identifier(:columns_table)
                            where table_schema = :TABLE_SCHEMA
                            and table_name     = :TABLE_NAME
                            and data_type in ('TIMESTAMP_TZ','VARIANT','OBJECT','ARRAY','GEOGRAPHY','GEOMETRY','VECTOR') AND ORDINAL_POSITION > 1);

        IF ( ARRAY_SIZE(INVALID_COLUMNS) > 0) THEN
           MSG := ('-- #ERROR THIS EXTERNAL TABLE HAS TYPES NOT SUPPORTED IN ICEBERG \n ' || ARRAY_TO_STRING(:INVALID_COLUMNS));
           UPDATE __external_tables_info 
           SET SCRIPTS = :MSG, INPROGRESS = FALSE
           
           WHERE TABLE_CATALOG = :TABLE_CATALOG AND 
                 TABLE_SCHEMA  = :TABLE_SCHEMA AND
                 TABLE_NAME    = :TABLE_NAME;
        ELSE
            LET priviledges VARCHAR := (SELECT ARRAY_TO_STRING(ARRAY_AGG(GRANT_STATEMENT),'\n') FROM 
            (SELECT 
                'GRANT ' || PRIVILEGE_TYPE || '  ON ' || OBJECT_CATALOG || '.' || OBJECT_SCHEMA || '.' || OBJECT_NAME 
                || ' TO ' || GRANTEE || IFF(is_grantable='YES',' WITH GRANT OPTION;'::VARCHAR(25),';'::VARCHAR(25)) GRANT_STATEMENT 
            FROM IDENTIFIER(:object_priviledges) WHERE
                    OBJECT_SCHEMA=:TABLE_SCHEMA and 
                    OBJECT_NAME=:TABLE_NAME and 
                    object_type='TABLE'));

            LET EXTERNAL_VOLUME_NAME VARCHAR := 'EXT_VOLUME_FOR_' || :TABLE_NAME ;
            LET CATALOG VARCHAR := 'parquet_direct_catalog';
            IF (TRIM(:TABLE_FORMAT) = 'DELTA' ) THEN
                CATALOG := 'delta_direct_catalog';
            END IF;
        
            IF (STARTSWITH(LOWER(URL),'s3')) THEN
            
                LET AWS_ROLE_ARN       VARCHAR   := (select "property_value" from table(result_scan(:query_id_stage)) where "property" = 'AWS_ROLE');
                LET AWS_EXTERNAL_ID    VARCHAR   := (select "property_value" from table(result_scan(:query_id_stage)) where "property" = 'AWS_EXTERNAL_ID');
                LET SNOWFLAKE_IAM_USER VARCHAR   := (select "property_value" from table(result_scan(:query_id_stage)) where "property" = 'SNOWFLAKE_IAM_USER');
                
                LET ICEBERG_STATEMENTS VARCHAR   :=
                '-- VOLUME \n' ||
                'CREATE OR REPLACE EXTERNAL VOLUME ' || :EXTERNAL_VOLUME_NAME || ' \n' ||
                'STORAGE_LOCATIONS = \n' ||
                '((\n' ||
                '            NAME = ''' || :EXTERNAL_VOLUME_NAME  || '''\n' ||
                '           STORAGE_PROVIDER = ''S3'' \n' ||
                '           STORAGE_BASE_URL = ''' || :URL || ''' \n' ||
                '           STORAGE_AWS_ROLE_ARN = ''' || :AWS_ROLE_ARN || ''' \n' ||
                '           STORAGE_AWS_EXTERNAL_ID = ''' || :AWS_EXTERNAL_ID || ''' \n' ||
                '        )\n' ||
                ') ALLOW_WRITES=FALSE;\n' ||
                '-- TABLE \n'||
                'CREATE OR REPLACE ICEBERG TABLE ' || :FULLNAME || '_EXT \n' ||
                'CATALOG = ' || :CATALOG || ' \n' ||
                'EXTERNAL_VOLUME = ' || :EXTERNAL_VOLUME_NAME || '\n ' ||
                'BASE_LOCATION='''' \n' ||
                '-- GRANTS \n' || priviledges || '\n\n';
                
               UPDATE __external_tables_info 
               SET SCRIPTS = :ICEBERG_STATEMENTS, INPROGRESS = FALSE
               WHERE TABLE_CATALOG = :TABLE_CATALOG AND 
                     TABLE_SCHEMA  = :TABLE_SCHEMA AND
                     TABLE_NAME    = :TABLE_NAME;
            ELSEIF (STARTSWITH(LOWER(:URL),'azure')) THEN
                LET INTEGRATION VARCHAR   := (select "property_value" from table(result_scan(:query_id_stage)) where "property" = 'STORAGE_INTEGRATION');
                DESCRIBE INTEGRATION IDENTIFIER(:INTEGRATION);
                LET query_id_integration := SQLID;
                LET AZURE_TENANT_ID       VARCHAR   := (select "property_value" from table(result_scan(:query_id_integration)) where "property" = 'AZURE_TENANT_ID');

                LET ICEBERG_STATEMENTS VARCHAR   :=
                '-- VOLUME \n' ||
                'CREATE OR REPLACE EXTERNAL VOLUME ' || :EXTERNAL_VOLUME_NAME || ' \n' ||
                'STORAGE_LOCATIONS = \n'||
                '((\n'||
                '           NAME = ''' || :EXTERNAL_VOLUME_NAME  || '''\n' ||
                '           STORAGE_PROVIDER = ''AZURE'' \n' ||
                '           STORAGE_BASE_URL = ''' || :URL || '''\n ' ||
                '           AZURE_TENANT_ID = ''' || :AZURE_TENANT_ID || '''\n' ||
                '        )\n' ||
                '    ) ALLOW_WRITES=FALSE;\n' ||
                '-- TABLE \n'||
                'CREATE OR REPLACE ICEBERG TABLE ' || :FULLNAME || '_EXT \n' ||
                'CATALOG = ' || :CATALOG || ' \n' ||
                'EXTERNAL_VOLUME = ' || :EXTERNAL_VOLUME_NAME || '\n ' ||
                'BASE_LOCATION='''' \n' ||
                '-- GRANTS \n' || priviledges || '\n\n';

             
              UPDATE __external_tables_info 
              SET SCRIPTS = :ICEBERG_STATEMENTS, INPROGRESS = FALSE
              WHERE TABLE_CATALOG = :TABLE_CATALOG AND 
                 TABLE_SCHEMA  = :TABLE_SCHEMA AND
                 TABLE_NAME    = :TABLE_NAME;
            
           END IF;
        END IF;
    EXCEPTION
         WHEN OTHER THEN
           UPDATE __external_tables_info 
           SET SCRIPTS=:sqlerrm,  INPROGRESS = FALSE
           WHERE TABLE_CATALOG = :TABLE_CATALOG AND 
                 TABLE_SCHEMA  = :TABLE_SCHEMA AND
                 TABLE_NAME    = :TABLE_NAME;
    END;
  END FOR ;
  -- DELETE TASK
  DROP TASK ICEBERG_MAKER_TASK;
  RETURN 0;
END;    
    """

def get_connection():
    return Session.builder.config("connection_name","cas2").getOrCreate() #.config("connection_name","cas2").
 
@st.cache_data
def get_databases():
    session = get_active_session()   
    return ["(all)"] + [x[0] for x in session.table("INFORMATION_SCHEMA.DATABASES").select("DATABASE_NAME").collect()]

@st.cache_data
def get_schemas(database_name):
    session = get_active_session()
    return ["(all)"] + [x[0] for x in session.table("INFORMATION_SCHEMA.SCHEMATA").where(col("CATALOG_NAME")==lit(database_name)).select("SCHEMA_NAME").collect()]

def get_external_tables(database_name,schema_name):
    session = get_active_session()   
    try:
        if database_name == "(all)":
            session.sql(f"show external tables in account ").write.mode("overwrite").save_as_table("_external_tables")
        else:
            if schema_name == "(all)":
                session.sql(f"show external tables in DATABASE \"{database_name}\" ").write.mode("overwrite").save_as_table("_external_tables")
            else:
                session.sql(f"use database \"{database_name}\"").show()
                session.sql(f"show external tables in SCHEMA \"{schema_name}\" ").write.mode("overwrite").save_as_table("_external_tables")    
        session.sql(f"""
        create or replace TABLE __EXTERNAL_TABLES_INFO (
        	PROCESS BOOLEAN,
        	TABLE_CATALOG VARCHAR(16777216),
        	TABLE_SCHEMA VARCHAR(16777216),
        	TABLE_NAME VARCHAR(16777216),
        	FILE_FORMAT_TYPE VARCHAR(16777216),
        	LOCATION VARCHAR(16777216),
        	FILE_FORMAT_NAME VARCHAR(16777216),
        	STAGE VARCHAR(16777216),
        	TABLE_FORMAT VARCHAR(16777216),
        	SCRIPTS VARCHAR(16777216) NOT NULL,
            INPROGRESS BOOLEAN
        );
        """).show()
        return session.sql(f"""
with 
    external_tables_info as (
        select "name" TABLE_NAME, "database_name" TABLE_CATALOG, "schema_name" TABLE_SCHEMA, "location" LOCATION ,"file_format_name",
        SPLIT("file_format_name",'.') format_parts,
        case array_size(format_parts)
          when 3 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
          when 2 then upper(TABLE_CATALOG || '.' || "file_format_name"                       ) 
          when 1 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
        end FILE_FORMAT_NAME,
        "file_format_type",
        "stage" STAGE,
        "table_format" TABLE_FORMAT
        from _external_tables et where "invalid" <> true
    ),
    file_formats as (
    select upper(FILE_FORMAT_CATALOG || '.' || FILE_FORMAT_SCHEMA || '.' || FILE_FORMAT_NAME) FILE_FORMAT_NAME,FILE_FORMAT_TYPE  
    from snowflake.account_usage.file_formats
    ),
    intermediate as (
    select 
        INFO.TABLE_CATALOG, 
        info.TABLE_SCHEMA, 
        info.TABLE_NAME, 
        COALESCE(info."file_format_type",ff.FILE_FORMAT_TYPE) FILE_FORMAT_TYPE, 
        info.LOCATION,
        info.FILE_FORMAT_NAME,
        info.STAGE,
        info.TABLE_FORMAT
    from external_tables_info info
    left join file_formats ff on
    info.FILE_FORMAT_NAME = ff.FILE_FORMAT_NAME)
    select FALSE PROCESS,*, '' SCRIPTS, FALSE INPROGRESS from intermediate 
    where TRIM(FILE_FORMAT_TYPE)='PARQUET' OR TRIM(TABLE_FORMAT)='DELTA'
        """).write.mode("append").save_as_table("__external_tables_info",block=False)
    except Exception as ex:
        st.error(str(ex))


st.title(f"Upgrading External Tables v1.0")
st.write("This app will help you upgrade your External tables to the Iceberg format")

session = get_active_session()

with st.sidebar:
    current_database = st.selectbox("Select Database", get_databases())

    if current_database:
        current_schema = st.selectbox("Select Schema", get_schemas(current_database))
   
st.info("To get started select a database and schema from the sidebar. And click 'Search For External Tables'. This process make time a few minutes")

creating_df_external_tables = False
if current_schema and st.button("Search For External Tables"):
    with st.spinner(f"Extracting External Tables details in database {current_database}..."):
        creating_df_external_tables = True
        session = get_active_session()
        async_job = None
        if current_database == "(all)":
            async_job=session.sql(f"show external tables in account ").write.mode("overwrite").save_as_table("_external_tables", block=False)
        else:
            if schema_name == "(all)":
                async_job = session.sql(f"show external tables in DATABASE \"{current_database}\" ").write.mode("overwrite").save_as_table("_external_tables",block=False)
            else:
                session.sql(f"use database \"{current_database}\"").show()
                async_job = session.sql(f"show external tables in SCHEMA \"{schema_name}\" ").write.mode("overwrite").save_as_table("_external_tables",block=False)    
        while (not async_job.is_done()):
           time.sleep(3)
    with st.spinner(f"Filling External Tables cache..."):
        
        session.sql(f"""
        create or replace TABLE __EXTERNAL_TABLES_INFO (
        	PROCESS BOOLEAN,
        	TABLE_CATALOG VARCHAR(16777216),
        	TABLE_SCHEMA VARCHAR(16777216),
        	TABLE_NAME VARCHAR(16777216),
        	FILE_FORMAT_TYPE VARCHAR(16777216),
        	LOCATION VARCHAR(16777216),
        	FILE_FORMAT_NAME VARCHAR(16777216),
        	STAGE VARCHAR(16777216),
        	TABLE_FORMAT VARCHAR(16777216),
        	SCRIPTS VARCHAR(16777216) NOT NULL,
            INPROGRESS BOOLEAN
        );
        """).show()
        async_job = session.sql(f"""
with 
    external_tables_info as (
        select "name" TABLE_NAME, "database_name" TABLE_CATALOG, "schema_name" TABLE_SCHEMA, "location" LOCATION ,"file_format_name",
        SPLIT("file_format_name",'.') format_parts,
        case array_size(format_parts)
          when 3 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
          when 2 then upper(TABLE_CATALOG || '.' || "file_format_name"                       ) 
          when 1 then upper(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || "file_format_name") 
        end FILE_FORMAT_NAME,
        "file_format_type",
        "stage" STAGE,
        "table_format" TABLE_FORMAT
        from _external_tables et where "invalid" <> true
    ),
    file_formats as (
    select upper(FILE_FORMAT_CATALOG || '.' || FILE_FORMAT_SCHEMA || '.' || FILE_FORMAT_NAME) FILE_FORMAT_NAME,FILE_FORMAT_TYPE  
    from snowflake.account_usage.file_formats
    ),
    intermediate as (
    select 
        INFO.TABLE_CATALOG, 
        info.TABLE_SCHEMA, 
        info.TABLE_NAME, 
        COALESCE(info."file_format_type",ff.FILE_FORMAT_TYPE) FILE_FORMAT_TYPE, 
        info.LOCATION,
        info.FILE_FORMAT_NAME,
        info.STAGE,
        info.TABLE_FORMAT
    from external_tables_info info
    left join file_formats ff on
    info.FILE_FORMAT_NAME = ff.FILE_FORMAT_NAME)
    select FALSE PROCESS,*, '' SCRIPTS, FALSE INPROGRESS from intermediate 
    where TRIM(FILE_FORMAT_TYPE)='PARQUET' OR TRIM(TABLE_FORMAT)='DELTA'
        """).write.mode("append").save_as_table("__external_tables_info",block=False)
        while (not async_job.is_done()):
           time.sleep(3)
        creating_df_external_tables = False


df_external_tables = None
try:
    if not creating_df_external_tables:
        df_external_tables = session.table("__external_tables_info").to_pandas()
except Exception as e:
    #st.error(e)
    pass

if df_external_tables is not None and len(df_external_tables) > 0:
    st.success(f" {len(df_external_tables)} found")
else:
    st.warning("No external tables found")
    st.stop()




previous_cols = df_external_tables.columns[1:]

selected = st.data_editor (df_external_tables, disabled=previous_cols)


selected_count = len(selected[selected["PROCESS"] == True])
if selected_count==0:
    st.stop()


if st.checkbox("SHOW CREATE STATEMENT for CATALOGs"):
    st.code(f"""
    CREATE OR REPLACE CATALOG INTEGRATION delta_direct_catalog
    CATALOG_SOURCE=OBJECT_STORE
    TABLE_FORMAT=DELTA
    ENABLED=TRUE;

    CREATE OR REPLACE CATALOG INTEGRATION parquet_direct_catalog
    CATALOG_SOURCE=OBJECT_STORE
    TABLE_FORMAT=NONE
    ENABLED=TRUE;
    ""","sql")
    
if st.button("PROCESS SELECTED"):
    # update table
    session.sql("""UPDATE __external_tables_info SET PROCESS = FALSE, INPROGRESS = TRUE""").show()
    with st.spinner("marking rows for processing..."):
        for idx, row in selected.iterrows():
            if row["PROCESS"] == True:
                session.sql("""
                UPDATE __external_tables_info SET 
                PROCESS = TRUE,
                SCRIPTS = '' 
                WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?""",
                params=[row['TABLE_CATALOG'], row['TABLE_SCHEMA'] , row['TABLE_NAME']]).show()
    # create task
    session.sql(TASK_SQL).show()
    # execute task
    session.sql("EXECUTE TASK ICEBERG_MAKER_TASK").show()
    st.info("TASK has been started")
    st.session_state.start_time = time.time()
if st.button("UPDATE"):
    if "start_time" in st.session_state:
        # Calculate elapsed time
        elapsed_time = time.time() - st.session_state.start_time
        # Display elapsed time in seconds
        st.info(f"Elapsed time: {elapsed_time:.2f} seconds")
    results = session.sql("SELECT * FROM __external_tables_info where PROCESS=TRUE").collect()
    for result in results:
        if result['INPROGRESS']:
            st.warning(f"Still in progress {result['TABLE_CATALOG']}.{result['TABLE_SCHEMA']}.{result['TABLE_NAME']}")
        else: 
            st.code(result['SCRIPTS'],"sql")