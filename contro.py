# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,import required libraries
import requests
import json
import  sqlalchemy as sa
import uuid
import time
from pyspark.sql.functions import current_timestamp, lit, max,lower


# COMMAND ----------

# MAGIC %run ./common/logger

# COMMAND ----------

# MAGIC %run ./common/config_db_connection

# COMMAND ----------

# MAGIC %run ./common/audit_db_connection

# COMMAND ----------

# MAGIC %run ./common/send_mail_functionality

# COMMAND ----------

# DBTITLE 1,Widgets at run time
dbutils.widgets.text('environment','stg','environment')#change in PROD
dbutils.widgets.text('source_snapshot','Current','source_snapshot')
dbutils.widgets.text('worker_job_id','','worker_job_id')#Json entry
dbutils.widgets.text('subject_area','CDL_RCC','subject_area')
dbutils.widgets.text('log_level','INFO','log_level') 
dbutils.widgets.text('cdl_batch_id','CDL_RCC_STUDIES_BATCH','cdl_batch_id')
dbutils.widgets.text('job_id','CDL_RCC_WORKFLOW','job_id')
dbutils.widgets.text('job_name','CDL_RCC_WORKFLOW','job_name')
dbutils.widgets.text('debug_mode','False','debug_mode')
dbutils.widgets.text('max_study_runtime','1','max_study_runtime')
dbutils.widgets.text('databricks_connection_id','gpd_databricks_stg','databricks_connection_id')

env = dbutils.widgets.get("environment")
source_snapshot = dbutils.widgets.get("source_snapshot")
cdl_batch_id = dbutils.widgets.get("cdl_batch_id")
subject_area = dbutils.widgets.get("subject_area")
log_level =  dbutils.widgets.get("log_level")
job_name= dbutils.widgets.get("job_name")
job_id = dbutils.widgets.get("job_id")
debug_mode = dbutils.widgets.get("debug_mode")
job_run_id =  str(uuid.uuid4().hex)
worker_job_id = int(dbutils.widgets.get("worker_job_id"))
max_study_runtime = int(dbutils.widgets.get("max_study_runtime"))
databricks_connection_id = dbutils.widgets.get("databricks_connection_id")

# COMMAND ----------

#creating config_client and reading job params
config_client = ConfigConnection(env=env,pool_size=1)
job_vars = config_client.get_job_vars(job_id)
config_client._set_env_vars()

#creating audit_client 
audit_client = AuditConnection(env=env,pool_size=2)
audit_client.cdl_batch_run_id =  str(uuid.uuid4().hex)
audit_client.cdl_job_run_id = job_run_id

# COMMAND ----------

# DBTITLE 1,Fetching required Job vars and Global vars 
token_scope = config_client.d_global_env_vars['databricks_token_scope']
token_key =  config_client.d_global_env_vars['databricks_token_key']
s3_conformed_cdl = config_client.d_global_env_vars['s3_conformed_cdl']
databricks_cdx_catalog = config_client.d_global_env_vars['databricks_cdx_catalog']
databricks_rcc_catalog = config_client.d_global_env_vars['databricks_rcc_catalog']
databricks_control_catalog = config_client.d_global_env_vars['databricks_control_catalog']
raw_to_aggregate_on_tables = job_vars.get('raw_to_aggregate_on_tables')
table_list_s3_path = job_vars.get('table_list_s3_path')
aggregate_crawler_params = job_vars.get('aggregate_crawler_params')
aggregate_uc_schema_name=job_vars.get('aggregate_uc_schema_name')
aggregate_file_path= job_vars.get('aggregate_file_path')
source_connection_id = job_vars.get('source_connection_id')
glue_connection_id = job_vars.get('glue_connection_id')
s3_connection_id = job_vars.get('s3_connection_id')
redshift_connection_id = job_vars.get('redshift_connection_id')
num_workers = job_vars.get('num_workers')
study_onboarding_params = job_vars.get('study_onboarding_params')
mysql_url = job_vars.get('mysql_url')
api_url = job_vars.get('api_url')
databricks_connection_params = config_client.get_connection_params(databricks_connection_id)
api_url = databricks_connection_params.get('api_url') + "/api/2.0/jobs/run-now"
logger = get_logger(job_name,log_level=log_level)
max_retries = 3
tries = 0


# COMMAND ----------

email_client = SendMail(env, job_id, audit_client.cdl_batch_run_id, job_run_id, job_name, config_client, logger)
email_from = config_client.d_global_env_vars['EMAIL_FROM']
email_to_cc = config_client.d_global_env_vars['EMAIL_TO_CC']
email_to = config_client.d_global_env_vars['EMAIL_TO']
email_client.set_params()

# COMMAND ----------

# DBTITLE 1,Checks for required metadata tables
#Checking the availability of the required metadata tables for clinical rcc pipeline processing

rcc_metadata_config_table = f'{databricks_rcc_catalog}.rcc_gpdip_metadata.rcc_study_config'
rcc_metadata_status_table = f'{databricks_rcc_catalog}.rcc_gpdip_metadata.rcc_study_status'
config = spark.catalog.tableExists(rcc_metadata_config_table)
status = spark.catalog.tableExists(rcc_metadata_status_table)
if config and status:
  logger.info(f"{rcc_metadata_config_table} and {rcc_metadata_status_table} are already present in the RCC catalog/schema")
else:
  email_body = f"<p>Hi All,</p><p>{rcc_metadata_config_table} or {rcc_metadata_status_table} or both tables are not available in the RCC catalog/schema</p>"
  email_subject = f"GPD Job Alert - {env} - {job_name} - Controller for RCC clinical"
  email_client.send_info_mail(email_from, email_to, email_to_cc, email_body, email_subject)  
  dbutils.notebook.exit("{rcc_metadata_config_table} or {rcc_metadata_status_table} or both tables are not available in the RCC catalog/schema")

# COMMAND ----------

# DBTITLE 1,Creating metadata table for incremental load processing
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {databricks_control_catalog}.gpdip_control")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {databricks_control_catalog}.gpdip_control.rcc_table_list(schema_name string,table_name string, studyid string , created_timestamp timestamp, updated_timestamp timestamp,last_modified_datetime timestamp) USING DELTA LOCATION '{table_list_s3_path}' TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' ='true')")

except Exception as e:
    logger.error(f"Unabe to create table or schema {databricks_control_catalog}.gpdip_control.rcc_table_list")
    raise (e)
 

# COMMAND ----------

# DBTITLE 1,Creation of table to maintain last_modified_datetime for incremental loads
rcc_table_list_df = spark.sql(f"select last_modified_datetime from {databricks_control_catalog}.gpdip_control.rcc_table_list where table_name ='rcc_study_config' and schema_name = 'rcc_gpdip_metadata'")
last_modified_datetime = rcc_table_list_df.collect()
if len(last_modified_datetime) == 0:
  last_modified_datetime = '1990-01-01 00:00:00.000000'
else:
  last_modified_datetime = last_modified_datetime[0][0]

logger.info(f"rcc_study_config will be using last_modified_datetime:{last_modified_datetime}")

# COMMAND ----------

# DBTITLE 1,Fetching studies which are to be onboarding and refreshed from Source table
onboard_study_sql="""
select distinct lower(config.study_id) as study_id, 
       config.schema_name, 
       collect_list(distinct nvl(config.table_name,'')) as table_list, 
       cast(nvl(status.migrated, false) as string) as migrated,
       max(status.updated) as updated from
          (select * 
          from {databricks_rcc_catalog}.rcc_gpdip_metadata.rcc_study_config  
          where updated > '{last_modified_datetime}'
          ) config
          inner join {databricks_rcc_catalog}.rcc_gpdip_metadata.rcc_study_status status 
          on config.study_id = status.study_id and config.schema_name = status.schema_name 
          where status.ready_for_processing is true and status.data_available is true and status.source = 'RCC' 
          group by config.study_id, config.schema_name, migrated, status.updated"""

onboard_study_df=spark.sql(onboard_study_sql.format(databricks_rcc_catalog=databricks_rcc_catalog,last_modified_datetime=last_modified_datetime))

if onboard_study_df.count() != 0:
   onboard_study_list = onboard_study_df.select('study_id').rdd.map(lambda x: x[0]).collect()
   onboard_study_string = "','".join(map(str, onboard_study_list))
   logger.info(f"Studies to be refreshed/onboarded are: {onboard_study_string}")
else:
   logger.info("No new study found to be refreshed or onboarded")
   dbutils.notebook.exit("No new study found to be refreshed or onboarded")

# COMMAND ----------

# DBTITLE 1,Query IP (Audit Table) to get the status of the existing studies and trigger the workflows for refreshed/new studies
try:
    #Query to get the status of the existing Studies
    last_ref_audit_qry= f"""
                        SELECT a.study_id study_id, audit_last_refreshed_ts, a.active_flag
                        FROM (SELECT study_id, active_flag FROM cdl_batch_config WHERE batch_id IN ('{cdl_batch_id}')) a
                        LEFT OUTER JOIN
                        (SELECT study_id, IFNULL(MAX(study_src_refresh_ts), TIMESTAMP('1985-01-01 00:00:00')) AS audit_last_refreshed_ts
                        FROM cdl_batch_run b
                        WHERE
                            batch_status != 'FAILED' AND lower(study_id) IN ('{onboard_study_string}')
                                AND subject_area = '{subject_area}'
                                AND study_id NOT IN (
                                    SELECT DISTINCT study_id FROM cdl_batch_run WHERE batch_status = 'STARTED'
                                    AND DATE_ADD(batch_start_ts, INTERVAL {max_study_runtime} HOUR) >= NOW()
                                    AND cdl_job_status != 'COMPLETED'
                                    AND subject_area = '{subject_area}'
                                )                              
                            GROUP BY study_id
                        ) b
                        ON a.study_id = b.study_id
                        ORDER BY audit_last_refreshed_ts DESC"""

    audit_existing_studies=[]
    l_studies_to_compare = []
    #Checking for Studies to be refreshed and creating a list out of it
    with audit_client.engine.connect() as conn:
        res = conn.execute(sa.text(last_ref_audit_qry))
        res_list=[r for r in res]
        if len(list(res_list))> 0:    
            for row in res_list:
                    study = row[0].lower()
                    audit_last_refreshed_ts = row[1]
                    active_flag = row[2]
                    audit_existing_studies.append(study)
                    if audit_last_refreshed_ts and active_flag == 'Y':
                        
                        study_detail = { 
                            'audit_last_refreshed_ts': audit_last_refreshed_ts,                  
                            'env' : env,
                            'debug_mode': str(debug_mode),
                            'study_id' : study,
                            'study_src_refresh_ts' :onboard_study_df.select('updated').filter(f"study_id = '{study}'").collect()[0][0],
                            'job_id' : job_id,
                            'job_name' : job_name,
                            'new_study_flag' : 'False',
                            'migrated_study' :onboard_study_df.select('migrated').filter(f"study_id = '{study}'").collect()[0][0].title(),
                            'batch_id' : cdl_batch_id,
                            'source_snapshot' : source_snapshot,
                            'rcc_tables'  : str(onboard_study_df.select('table_list').filter(f"study_id = '{study}'").collect()[0][0]),
                            'glue_connection_id' : glue_connection_id,
                            's3_connection_id': s3_connection_id,
                            'redshift_connection_id':redshift_connection_id,
                            'num_workers' : num_workers,
                            'study_onboarding_params':study_onboarding_params,
                            'databricks_cdx_catalog' : databricks_cdx_catalog,
                            'databricks_rcc_catalog':databricks_rcc_catalog,
                            'databricks_control_catalog': databricks_control_catalog,
                            'aggregate_crawler_params':aggregate_crawler_params,
                            'raw_schema_name' :  onboard_study_df.select('schema_name').filter(f"study_id = '{study}'").collect()[0][0],
                            's3_conformed_cdl' : s3_conformed_cdl,
                            'aggregate_file_path' : aggregate_file_path,
                            'raw_to_aggregate_on_tables' : raw_to_aggregate_on_tables,
                            'aggregate_uc_schema_name': aggregate_uc_schema_name,
                            'table_list_s3_path':table_list_s3_path
                            }

                        
                        
                        l_studies_to_compare.append(study_detail)
                    
        else:
            logger.info("No Study returned to be refreshed as per IP mysql audit tables")            
            res.close()
            

    def generate_run_params(study_id):
        """Function to generate runtime params for the New studies and return a dictionary of params for each study :return: params dictionary"""
        params = {             
            'env' : env,
            'debug_mode': str(debug_mode),
            'study_id' : study_id,
            'study_src_refresh_ts' :onboard_study_df.select('updated').filter(f"study_id = '{study_id}'").collect()[0][0],
            'job_id' : job_id,
            'job_name' : job_name,
            'new_study_flag' : 'True',
            'migrated_study' :onboard_study_df.select('migrated').filter(f"study_id = '{study_id}'").collect()[0][0].title(),
            'batch_id' : cdl_batch_id,
            'source_snapshot' : source_snapshot,
            'rcc_tables'  : str(onboard_study_df.select('table_list').filter(f"study_id = '{study_id}'").collect()[0][0]),
            'glue_connection_id' : glue_connection_id,
            's3_connection_id': s3_connection_id,
            'redshift_connection_id':redshift_connection_id,
            'num_workers' : num_workers,
            'study_onboarding_params':study_onboarding_params,
            'databricks_cdx_catalog' : databricks_cdx_catalog,
            'databricks_rcc_catalog':databricks_rcc_catalog,
            'databricks_control_catalog': databricks_control_catalog,
            'aggregate_crawler_params':aggregate_crawler_params,
            'raw_schema_name' :  onboard_study_df.select('schema_name').filter(f"study_id = '{study_id}'").collect()[0][0],
            's3_conformed_cdl' : s3_conformed_cdl,
            'aggregate_file_path' : aggregate_file_path,
            'raw_to_aggregate_on_tables' : raw_to_aggregate_on_tables,
            'aggregate_uc_schema_name': aggregate_uc_schema_name,
            'table_list_s3_path':table_list_s3_path
        }
        return params
    #Creating params for new studies
    new_studies_runtime_params = [ generate_run_params(study_id) for study_id in onboard_study_list if study_id not in audit_existing_studies]
    
    new_studies = ', '.join([new_study_param.get('study_id') for new_study_param in new_studies_runtime_params])
    logger.info(f'Newly added studies in RCC =>  {new_studies}')

    def compare_refresh_ts(study_refresh_detail):
        """compare the refresh timestamp and returns runtime params for the studies need to be ingested :return study_refresh_detail dictionary"""
        if study_refresh_detail.get('audit_last_refreshed_ts') is None or study_refresh_detail.get('study_src_refresh_ts') > study_refresh_detail.get('audit_last_refreshed_ts') :
            if study_refresh_detail.get("audit_last_refreshed_ts"):
                del study_refresh_detail["audit_last_refreshed_ts"]
            study_refresh_detail["study_src_refresh_ts"] = study_refresh_detail.get(
                "study_src_refresh_ts")

            return study_refresh_detail
        else:
            return None

    existing_studies_runtime_params = list(filter(None, map(compare_refresh_ts, l_studies_to_compare)))
    existing_studies = ', '.join([str({"STUDY_ID": study_param.get('study_id'),
                                    "study_src_refresh_ts": study_param.get('study_src_refresh_ts')})
                                for study_param in existing_studies_runtime_params])
    logger.info(f'Studies refreshed in RCC since last IP Refresh RCC with the source last refresh time => {existing_studies}')

    l_runtime_params = []
    if new_studies_runtime_params:
        l_runtime_params.extend(new_studies_runtime_params)
        logger.info(f"New study will be triggered as a part of this DAG run {new_studies_runtime_params}")
        
    if existing_studies_runtime_params:
       
            l_runtime_params.extend(existing_studies_runtime_params)

    studies_triggered = ', '.join([run_param.get('study_id') for run_param in l_runtime_params])

    logger.info(f"Studies triggered {studies_triggered}")

    t=dbutils.secrets.get(scope=token_scope, key=token_key)
    token_extra = json.loads(t)
    token = token_extra.get('token')
    response = ''
    '''
    Status code to retry on:
    429: Too many requests,
    500: Internal Server error,
    502: Bad Gateway,
    503: Service unavailability
    504: Gateway Timeout
    '''
    api_retry_status_codes = [429, 500, 502, 503, 504]
    api_call_successful = True
    if len(l_runtime_params) > 0:
        #Trigger workflows through Databricks API for each study
        for runtime_param in l_runtime_params:
            api_exception = ''
            #Functionality to retry API hits:
            while tries < max_retries:
                runtime_param['study_src_refresh_ts']=runtime_param['study_src_refresh_ts'].isoformat()
                try:
                    payload = {
                        "job_id": worker_job_id,
                        "notebook_params": runtime_param
                    }
                    response = requests.post(api_url, headers = {'Authorization': 'Bearer ' + token}, data=json.dumps(payload))
                    logger.debug(f"Run for Study {runtime_param['study_id']} with notebook params as {payload} ")
                    logger.info(f"Response: {response.json()}")
                    if response.status_code in api_retry_status_codes:
                        logger.info(f"Retry count: {tries + 1} :API called failed while invoking worker job for study {runtime_param['study_id']} with status: {response.status_code} and response: {response.json()}.Retrying back in 10 secs.")
                        tries += 1
                        time.sleep(10)
                        continue
                    else:
                        logger.info(f"Retry count: {tries + 1} :Worker job API invoked for study {runtime_param['study_id']} with status: {response.status_code} and response: {response.json()}. No retry would be triggered.")
                        break
                except Exception as e:
                    logger.error(f"Exception occured while invoking worker job for study {runtime_param['study_id']}: {e} at retry count: {tries+1} ")
                    api_exception = e
                    break
            if api_exception or response.status_code != 200:
                api_call_successful = False
                logger.info(f"Run for Study {runtime_param['study_id']} with notebook params failed")

                email_body = f"<p>Hi All,</p><p>Run for Study {runtime_param['study_id']} failed while invoking worker job: {response.json() if not api_exception else api_exception}. In the subsequent run, this study will be reprocessed.</p>"
                email_subject = f"GPD Job Failure Alert - {env} - {job_name} - Study processing for {runtime_param['study_id']}"
                email_client.send_info_mail(email_from, email_to, email_to_cc, email_body, email_subject)

        if api_call_successful:
            #Only if worker jobs were triggered (with status:200) -> Update rcc_table_list with latest modified date of rcc_study_config
            rcc_table_list_df = spark.sql(f"SELECT * FROM {databricks_control_catalog}.gpdip_control.rcc_table_list WHERE schema_name = 'rcc_gpdip_metadata' and table_name = 'rcc_study_config'")
            max_modified_datetime = spark.sql(f"select max(updated) from {databricks_rcc_catalog}.rcc_gpdip_metadata.rcc_study_config ").collect()[0][0]

            if rcc_table_list_df.count() == 0:
                spark.sql(f"INSERT INTO {databricks_control_catalog}.gpdip_control.rcc_table_list (schema_name ,table_name , studyid  , created_timestamp , updated_timestamp ,last_modified_datetime ) VALUES ('rcc_gpdip_metadata', 'rcc_study_config', null, current_timestamp(), current_timestamp(), cast('{max_modified_datetime}' as timestamp))")
            else:
                latest_table_list_df = rcc_table_list_df.withColumn('last_modified_datetime', lit(max_modified_datetime).cast('timestamp')).withColumn('updated_timestamp', current_timestamp())
                latest_table_list_df.write.format("delta").mode("overwrite").option("replaceWhere", f"schema_name = 'rcc_gpdip_metadata' and table_name = 'rcc_study_config'").save(table_list_s3_path)

    else:

        logger.info(f"No studies triggered since the all the studies were refreshed already for the current run")

except Exception as e:
    logger.info(f"Exception {e} raised while executing code to create params and trigger studies")
    raise Exception(e)
