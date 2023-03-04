from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkFiles
from datetime import datetime
from SparkLogger import SparkLogger
from pyspark.sql.types import TimestampType, IntegerType, DateType, StringType, StructField, StructType
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from pyspark.storagelevel import StorageLevel

VALIDATION_TABLE="spark.se2bq.validation.table"
VALIDATION_DATASET="spark.se2bq.validation.dataset"
VALIDATION_PARTITION_COLUMN="spark.se2bq.validation.column"
TEMPORARY_BUCKET="spark.se2bq.tempbucket"
PARTITION_COLUMN="spark.se2bq.paritioncolumn"
DATASET="spark.se2bq.dataset"
TABLE_LIST="spark.se2bq.tables"
INCREMENTAL_DATE="spark.se2bq.incrementaldate"
CREDENTIALS_FILE="spark.se2bq.cred.file"


def create_validation_df(spark, table_name, source_row_count, destination_row_count, count_result, dataset, load_date, job_start_time, job_end_time, total_runtime, log_details):
    
    input_data = [(table_name, source_row_count, destination_row_count, count_result, dataset, load_date, job_start_time, job_end_time, total_runtime, log_details)]
    
    schema = StructType([ \
    StructField("table_name", StringType(), False), \
    StructField("source_row_count", StringType(), False), \
    StructField("destination_row_count", StringType(), False), \
    StructField("count_result", StringType(), False), \
    StructField("database_name", StringType(), False), \
    StructField("load_date", DateType(), False), \
    StructField("job_start_time", TimestampType(), False), \
    StructField("job_end_time", TimestampType(), False), \
    StructField("total_runtime", StringType(), False), \
    StructField("log_details", StringType(), False)
  ])
    
    df = spark.createDataFrame(data=input_data, schema=schema)
    return df


def calculateTimeDiff(job_start_time, job_end_time):
    duration = job_end_time - job_start_time
    duration_in_s = duration.total_seconds()
    
    if duration_in_s < 60 :
        return f'{int(duration_in_s)} seconds'
    
    minutes = divmod(duration_in_s, 60)
    
    return f'{int(minutes[0])} minutes {int(minutes[1])} seconds'

def tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False

def deletExistingRows(credentials, partition_column, cdate, dataset, table):
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        table_ref = f'{credentials.project_id}.{dataset}.{table}'
        logger.info(f'================== Check if data for {cdate} was already replicated to BigQuery ==================')
        if tbl_exists(client, table_ref) :
            logger.info(f'==== Deleting Existing Data in {dataset}.{table}` for {cdate} to avoid duplications =====')
            query_delete = f"delete from `{dataset}.{table}` where {partition_column} = '{cdate}'"
            job = client.query(query_delete)
            job.result()
    except Exception as e:
        logger.error(f'Error in trying to check if the table exists or deleting existing data :::: {dataset}.{table}')
        logger.error(f'Error  :::: {e}')
        
def fetchTargetCount(credentials, where_cond, dataset, table):
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        table_ref = f'{credentials.project_id}.{dataset}.{table}'
        count = None
        if tbl_exists(client, table_ref) :
            query_count = f"select count(*) size from `{dataset}.{table}` where {where_cond}"
            job = client.query(query_count)
            results = job.result()
            
            for row in results:
                count = str(row.size)
    
        return count
    
    except Exception as e:
        logger.error(f'Error in fetching count :::: {table}')
        logger.error(f'Error  :::: {e}')
        
if __name__ == "__main__" :
    
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    logger = SparkLogger(spark)  
    spark_config = spark.conf
    validation_table=spark_config.get(VALIDATION_TABLE)
    validation_dataset=spark_config.get(VALIDATION_DATASET)  
    validation_partition_column=spark_config.get(VALIDATION_PARTITION_COLUMN)
    bucket=spark_config.get(TEMPORARY_BUCKET)  
    partition_column=spark_config.get(PARTITION_COLUMN)
    dataset=spark_config.get(DATASET)
    incremental_date=spark_config.get(INCREMENTAL_DATE)
    cred_file=spark_config.get(CREDENTIALS_FILE)
    
    service_account_df = spark.read.text(f'file://{cred_file}',wholetext=True)
    
    service_account_df=json.loads(str(service_account_df.collect()[0][0]),strict=False)
    
    credentials = service_account.Credentials.from_service_account_info(service_account_df)
    
    sa_key_id = service_account_df['private_key_id']
    sa_key = service_account_df['private_key']
    sa_email = service_account_df['client_email']
    
    table_list=spark_config.get(TABLE_LIST).strip().split(",")
    
    spark.conf.set("viewsEnabled","true")
    spark.conf.set('temporaryGcsBucket',bucket)
    spark.conf.set('credentialsFile',cred_file)
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key.id", sa_key_id)
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key", sa_key)
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email", sa_email)
    
    current_date = datetime.today()
    cdate = current_date.strftime('%Y-%m-%d')
    
    if incremental_date != 'NONE' :
        cdate = incremental_date

    for table in table_list :
        job_start_time = datetime.now()
        source_count = '0'
        destination_count = '0'
        try:
            logger.info(f'Start replication of table ::::::: {dataset}.{table} ')
            cda_year = int(cdate.split('-')[0])
            where_cond = f"cda_year = {cda_year} and {partition_column}='{cdate}'"
            
            if incremental_date == '0000-01-01' :
                where_cond = f"{partition_column}>='{cdate}'"
                
            
            hive_sql=f"SELECT * FROM {dataset}.{table} where {where_cond}"
            
            logger.info(f'================== Extracting Source Data ==================')
            logger.info(f'==== Executing query: {hive_sql} =====')
            
            df = spark.sql(hive_sql)
            source_count = str(df.count())

            logger.info(f'==== Source count for {dataset}.{table}: {source_count} =====')
            
            if source_count == '0' :
                logger.info(f'Table is empty in source ::::::: {dataset}.{table} ')
            else :
                if incremental_date != '0000-01-01' :
                    deletExistingRows(credentials, partition_column, cdate, dataset, table)
                    
                logger.info(f'================== Replicating Data from SE to BigQuery ==================')
                logger.info(f'==== Partition Field: {partition_column} =====')
                df.write.format("bigquery")\
                    .option('intermediateFormat','orc')\
                    .option("allowFieldAddition","true")\
                    .option("partitionRequireFilter","true")\
                    .option("partitionField",partition_column)\
                    .option("partitionType","DAY")\
                    .option("parentProject",credentials.project_id)\
                    .option("dataset",dataset)\
                    .option("table",table).mode("append").save()
                
                count_filter_dt = cdate
                
                where_val_cond = f"cda_year = {cda_year} and {partition_column}='{count_filter_dt}'"
                
                if incremental_date == '0000-01-01' :
                    count_filter_dt = '0001-01-01'
                    where_val_cond = f"{partition_column}>='{count_filter_dt}'"

                logger.info(f'==== Check count of the data in BigQuery =====')
                logger.info(f'==== Dataset: {dataset}; Table: {table}; Filter used: {where_val_cond} =====')
                
                destination_count = fetchTargetCount(credentials, where_val_cond, dataset, table)
                
                logger.info(f'==== Destination count for {dataset}.{table}: {destination_count} =====')
                
            job_end_time = datetime.now()
            
            log_details = ''
            
            count_result = 'Pass'
    
            if source_count != destination_count:
                count_result = 'Fail'
                logger.info(f'==== Table not loaded correctly, please check ::::  {table} >> {count_result} =====')
            else:
                logger.info(f'Successfully Loaded Table:{table}') #do not change the format of this line as it is used in UC4 jobs

            logger.info(f'==== Is the count in SE equal to the count in BigQuery? ::::  {count_result} =====')

            total_runtime = calculateTimeDiff(job_start_time, job_end_time)
            
            df_validation = create_validation_df(spark, table, source_count, destination_count, count_result, dataset, current_date, job_start_time, job_end_time, total_runtime, log_details)
            
            logger.info(f'================== Write information in the Validation table ==================')
            
            df_validation.write.format("bigquery")\
                .option('intermediateFormat','orc')\
                .option("allowFieldAddition","true")\
                .option("partitionRequireFilter","true")\
                .option("partitionField",validation_partition_column)\
                .option("partitionType","DAY")\
                .option("parentProject",credentials.project_id)\
                .option("dataset",validation_dataset)\
                .option("table",validation_table).mode("append").save()
                
            spark.catalog.clearCache()
            logger.info(f'Ended Replicating Table ::::::: {table} ')
        except Exception as e:
            logger.error(f'Error in loading table :::: {dataset}.{table}')
            logger.error(f'Error  :::: {e}')
            
            try :
                logger.error(f'Add error details in validation table :::: {dataset}.{table}')
                
                job_end_time = datetime.now()
                
                log_details = str(e)
                count_result = 'Fail'
                total_runtime = calculateTimeDiff(job_start_time, job_end_time)
                df_validation = create_validation_df(spark, table, source_count, destination_count, count_result, dataset, current_date, job_start_time, job_end_time, total_runtime, log_details)
                
                df_validation.write.format("bigquery")\
                    .option('intermediateFormat','orc')\
                    .option("allowFieldAddition","true")\
                    .option("partitionRequireFilter","true")\
                    .option("partitionField",validation_partition_column)\
                    .option("partitionType","DAY")\
                    .option("parentProject",credentials.project_id)\
                    .option("dataset",validation_dataset)\
                    .option("table",validation_table).mode("append").save()
            except Exception as e:
                logger.error(f'Error while loading error details in validation table :::: {dataset}.{table} :::: {e}')