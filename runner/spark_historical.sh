#!/usr/bin/env bash

basedir=/opt/apps/cda/deploy/se2bigquery
source $basedir/common/configs.sh
START=$(date +%s)
export PYSPARK_SCRIPT=$1
export CONFIG_FILE=$2

export load_date=0000-01-01

executor_params

echo "executor parameters :: $executorsParams"

# Start Spark Application
spark-submit \
	 --driver-java-options "-Dlog4j.configuration=file:$log4j_properties -Dlogfile.name=se2bq -Dspark.yarn.app.container.log.dir=/var/log/se2bigquery" \
	 --properties-file $CONFIG_FILE \
	 --conf spark.sql.hive.conf.list="hive.vectorized.execution.enabled=true" --conf spark.se2bq.incrementaldate=$load_date \
	 --conf spark.se2bq.projectid=$gcp_project_id \
	 --conf spark.se2bq.tempbucket=$gcp_temp_bucket \
	 --conf spark.se2bq.cred.file=$gcp_sa_file \
	 --conf spark.sql.hive.hiveserver2.jdbc.url=$hiveserver_jdbc \
	 --conf spark.yarn.keytab=/etc/security/keytabs/$USER.keytab \
	 --conf spark.yarn.principal=$USER@$PRINCIPAL_FQDN \
	 --conf spark.ui.enabled=false \
	 --conf spark.sql.hive.convertMetastoreOrc=true \
	 --conf spark.sql.orc.enabled=true \
	 --conf spark.security.credentials.hiveserver2.enabled=false \
	 --master yarn \
	 --deploy-mode client \
	 --jars $hwc_jar_path,$gcp_jar,$gcp_bq_jar \
	 --py-files $hwc_zip_path \
	 $executorsParams --queue=landing_900 $SE2BQ_BASE_DIR/src/$PYSPARK_SCRIPT

export returnCode=${PIPESTATUS[0]}
echo "ReturnCode in SparkSubmit function.......$returnCode"
END=$(date +%s)
duration=$(( $END - $START ))
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds"

### start evaluation
validateRun