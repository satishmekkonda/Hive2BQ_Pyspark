#!/usr/bin/env bash

export USER=ta2sprz
export ENV_TYPE={{envtype}}
echo $ENV_TYPE
export gcp_project_id={{gcp_project_id}}
export gcp_temp_bucket={{gcp_temp_bucket}}
export hiveserver_jdbc={{hiveserver_jdbc}}
echo "##### Configs for "$ENV_TYPE" used which are defined in CDP-SCMDB - If it is not the correct environment. Please contact landing team."

export SE2BQ_BASE_DIR=/opt/apps/cda/deploy/se2bigquery
export GCP_CONFIG=/var/iophome/ta2sprz/.se2bigquery
source /opt/apps/cda/deploy/platform-env-conf/conf/env/env-conf.sh
source /opt/apps/cda/deploy/platform-env-conf/conf/env/python3-conf.sh
export YARN_HOME=${YARN_HOME="/usr/hdp/current/hadoop-yarn-client"}
kinit -kt /etc/security/keytabs/$USER.keytab $USER@$PRINCIPAL_FQDN
export log4j_properties=$SE2BQ_BASE_DIR/config/log4j.properties
export gcp_bq_jar=$SE2BQ_BASE_DIR/jars/spark-bigquery-with-dependencies_2.11-0.24.2.jar
export gcp_jar=$SE2BQ_BASE_DIR/jars/gcs-connector-hadoop2-latest.jar
export gcp_sa_file=$GCP_CONFIG/gcp_key.json

hwc_jar_path=$(find /usr/hdp/current/hive_warehouse_connector/ -iname "*.jar")
hwc_zip_path=$(find /usr/hdp/current/hive_warehouse_connector/ -iname "*.zip")

function executor_params () {

case $ENV_TYPE in

    "DEV") executorsParams="--num-executors 4 --executor-cores 10 --executor-memory 4g --driver-memory 2g"
    echo $executorsParams
    ;;
    
    "DEV-ENTW") executorsParams="--num-executors 4 --executor-cores 10 --executor-memory 4g --driver-memory 2g"
    echo $executorsParams
    ;;
    
    "PROD") executorsParams="--num-executors 10 --executor-cores 4 --executor-memory 28G --driver-memory 10G"
    echo $executorsParams
    ;;

    *) executorsParams="--num-executors 4 --executor-cores 10 --executor-memory 4g --driver-memory 2g"
    echo $executorsParams
    ;;
    esac
}


function validateRun() {
# check the return code of the spark call and validate it
echo "Returncode in validateRun.....$returnCode"

if [ "${returnCode}" == 0 ]; then
	echo "----------------"
	echo "No issues found."
	echo ""
	echo "Start: $START"
	echo "End:   $END"
	echo "----------------"
else
	echo "----------------"
  echo "Issue while executing spark script"
	echo ""
	echo "Start: $START"
	echo "End:   $END"
	echo "----------------"
	exit 1
fi
}