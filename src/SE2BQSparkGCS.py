from pyspark.sql import SparkSession
from pyspark import SparkConf
from SparkLogger import SparkLogger
import json

BUCKET="spark.se2bq.bucket"
FILE_LIST="spark.se2bq.files"
CREDENTIALS_FILE="spark.se2bq.cred.file"

if __name__ == "__main__" :
    
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    logger = SparkLogger(spark)  
    spark_config = spark.conf
    bucket=spark_config.get(BUCKET)  
    cred_file=spark_config.get(CREDENTIALS_FILE)
    
    service_account_df = spark.read.text(f'file://{cred_file}',wholetext=True)
    
    service_account_df=json.loads(str(service_account_df.collect()[0][0]),strict=False)
    
    sa_key_id = service_account_df['private_key_id']
    sa_key = service_account_df['private_key']
    sa_email = service_account_df['client_email']
    

    file_list=spark_config.get(FILE_LIST).strip().split(",")
    
    spark.conf.set('credentialsFile',cred_file)
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key.id", sa_key_id)
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key", sa_key)
    spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email", sa_email)
    
    for file in file_list :
        try:
            logger.info(f'Start File ::::::: {file} ')
            
            df = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(file)
            
            df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(f'gs://{bucket}/{file}')
            
            # df.write.format("csv").save(f'gs://{bucket}/{file}')
        
            logger.info(f'End File ::::::: {file} ')
            
        except Exception as e:
            logger.error(f'Error in loading file :::: {file}')
            logger.error(f'Error  :::: {e}')