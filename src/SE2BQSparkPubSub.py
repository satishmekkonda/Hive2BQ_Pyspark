from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkFiles
from datetime import datetime
from SparkLogger import SparkLogger
from pyspark.sql.types import TimestampType, IntegerType, DateType, StringType, StructField, StructType
import json
from google.oauth2 import service_account
from datetime import datetime
from google.cloud import pubsub_v1
from google.cloud import bigquery
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from google.cloud.exceptions import NotFound

VALIDATION_TABLE="spark.se2bq.validation.table"
VALIDATION_DATASET="spark.se2bq.validation.dataset"
VALIDATION_PARTITION_COLUMN="spark.se2bq.validation.column"
TEMPORARY_BUCKET="spark.se2bq.tempbucket"
PARTITION_COLUMN="spark.se2bq.paritioncolumn"
DATASET="spark.se2bq.dataset"
TABLE_LIST="spark.se2bq.tables"
INCREMENTAL_DATE="spark.se2bq.incrementaldate"
CREDENTIALS_FILE="spark.se2bq.cred.file"
PROCESS_DB_JDBC_URL="spark.se2bq.processdb.jdbc.url"
PROCESS_DB_USERNAME="spark.se2bq.processdb.username"
PROCESS_DB_PASSWORD="spark.se2bq.processdb.password"
PROCESS_DB_SQL="spark.se2bq.processdb.sql"
PUBSUB_TOPIC_PATH="spark.se2bq.pubsub.topic"
PUBSUB_TABLE="spark.se2bq.pubsub.table"
PUBSUB_PARTITION_COLUMN="spark.se2bq.pubsub.column"
PUBSUB_DATASET="spark.se2bq.pubsub.dataset"

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

def fetchPubsubMessage(spark, cdate, jdbc_url, processdb_username, processdb_password, sql, database, status, tablename):

    cdate_datetime=datetime.strptime(cdate, '%Y-%m-%d')
    cdate_int=int(cdate_datetime.strftime("%Y%m%d"))
    
    sql = sql.replace("<SOURCE_TABLE>", tablename).replace("<TARGET_DB>", database).replace("<CDATE>", str(cdate_int))
    
    df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"({sql}) as tbl").option("user", processdb_username).option("password", processdb_password).load()
    
    if df.count() == 0 :
        return None, None
    
    message_dict = {}
    
    dataCollect = df.collect()
    for row in dataCollect:
        for field in df.schema.fields:
            data_value=row[field.name]
            data_type=field.dataType
            message_dict[field.name]=str(data_value)
    
    message_dict['source_system']=database
    message_dict['table_name']=tablename
    message_dict['count_result']=status
    
    message_json = json.dumps(message_dict)
    
    pubsub_df = df.withColumn("source_system",lit(database)).withColumn("table_name",lit(tablename)).withColumn("count_result",lit(status)).withColumn("cda_date", to_date(lit(cdate_datetime)))
    
    return message_json, pubsub_df


def deletExistingRows(credentials, partition_column, cdate, dataset, table):
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        query_delete = f"delete from `{dataset}.{table}` where {partition_column} >= '{cdate}'"
        job = client.query(query_delete)
        job.result()
        logger.info(f'==================Deleted========================== {query_delete}')
    
    except Exception as e:
        logger.error(f'Error in deleting existing data :::: {table}')
        logger.error(f'Error  :::: {e}')   

def tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False
    
def createTableIfNotExists(spark, credentials, partition_column, schema, dataset, table):
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        table_ref = f'{credentials.project_id}.{dataset}.{table}'
        if not tbl_exists(client, table_ref) :
            df_empty = spark.createDataFrame([],schema=schema)
            logger.info(f'====================EMPTY TABLE CREATING==================================================')
            df_empty.write.format("bigquery")\
                .option('intermediateFormat','orc')\
                .option("allowFieldAddition","true")\
                .option("partitionRequireFilter","true")\
                .option("partitionField",partition_column)\
                .option("partitionType","DAY")\
                .option("parentProject",credentials.project_id)\
                .option("dataset",dataset)\
                .option("table",table).mode("append").save()
            logger.info(f'=========================EMPTY TABLE CREATED=============================================')
    
    except Exception as e:
        logger.error(f'Error in creating empty table in bigquery :::: {table}')
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
    
    processdb_jdbc=spark_config.get(PROCESS_DB_JDBC_URL)
    processdb_username=spark_config.get(PROCESS_DB_USERNAME)
    processdb_password=spark_config.get(PROCESS_DB_PASSWORD)
    processdb_sql=spark_config.get(PROCESS_DB_SQL)
    
    pubsub_topic=spark_config.get(PUBSUB_TOPIC_PATH)
    
    pubsub_table=spark_config.get(PUBSUB_TABLE)
    pubsub_partition_column=spark_config.get(PUBSUB_PARTITION_COLUMN)
    pubsub_dataset=spark_config.get(PUBSUB_DATASET)
    
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
        try:
            logger.info(f'Start Table ::::::: {table} ')
            
            hive_sql=f"SELECT * FROM {dataset}.{table} where {partition_column}>='{cdate}'"
            
            df = spark.sql(hive_sql)
            
            source_count = str(df.count())
            
            destination_count = '0'
            
            if source_count == '0' :
                logger.info(f'======================================================================')
                logger.info(f'Table is empty in source ::::::: {table} ')
                createTableIfNotExists(spark, credentials, partition_column, df.schema, dataset, table)
                
            else :
                if incremental_date != '0000-01-01' :
                    deletExistingRows(credentials, partition_column, cdate, dataset, table)
                
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
                
                if incremental_date == '0000-01-01' :
                    count_filter_dt = '0001-01-01'
            
                df_bq_count = spark.read \
                  .format("bigquery") \
                  .option("dataset",dataset) \
                  .option("parentProject",credentials.project_id)\
                  .option("filter",f"{partition_column}>='{count_filter_dt}'") \
                  .load(table)
                  
                destination_count = str(df_bq_count.count())
                
            job_end_time = datetime.now()
            
            log_details = ''
            
            count_result = 'Pass'
    
            if source_count != destination_count:
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
            
            
            if incremental_date != '0000-01-01' :
                
                message_json, pubsub_df = fetchPubsubMessage(spark, cdate, processdb_jdbc, processdb_username, processdb_password, processdb_sql, dataset, count_result, table)
                
                if message_json is not None :
                    
                    
                    pubsub_df.write.format("bigquery")\
                        .option('intermediateFormat','orc')\
                        .option("allowFieldAddition","true")\
                        .option("partitionRequireFilter","true")\
                        .option("partitionField",pubsub_partition_column)\
                        .option("partitionType","DAY")\
                        .option("parentProject",credentials.project_id)\
                        .option("dataset",pubsub_dataset)\
                        .option("table",pubsub_table).mode("append").save()
                    
                    publisher = pubsub_v1.PublisherClient(credentials=credentials)
                    future = publisher.publish(pubsub_topic, data=message_json.encode("utf-8"))
                    logger.info(future.result())
            
            logger.info(f'End Table ::::::: {table} ')
            spark.catalog.clearCache()
        except Exception as e:
            logger.error(f'Error in loading table :::: {dataset}.{table}')
            logger.error(f'Error  :::: {e}')
            
            try :
                destination_count = '0'
                
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