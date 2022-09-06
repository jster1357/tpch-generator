#!/bin/bash

function usage {
	echo "Usage: tpch-datagen.sh scale_factor bucket"
	exit 1
}

function runcommand {
	$1
}

if [ ! -f target/target/tpch-gen-1.0-SNAPSHOT.jar ]; then
	echo "Please build the data generator with ./tpch-build.sh first"
	exit 1
fi
which hive > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Script must be run where Hive is installed"
	exit 1
fi

# Get the parameters.
SCALE=$1
BUCKET=$2
DATA_FORMAT=$3

if [ "X$DEBUG_SCRIPT" != "X" ]; then
	set -x
fi

# Sanity checking.
if [ X"$SCALE" = "X" ]; then
	usage
fi
if [ $SCALE -eq 1 ]; then
	echo "Scale factor must be greater than 1"
	exit 1
fi

# Do the actual data load.
DIR=/tmp/tpch-generate
#DIR=s3a://jtaras-hwx/tpch-generate
hadoop fs -mkdir -p ${DIR}
hadoop fs  -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Generating data at scale factor $SCALE."
	(cd target; hadoop jar target/*.jar -d ${DIR}/${SCALE}/ -s ${SCALE} -text)
fi
hadoop fs -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Data generation failed, exiting."
	exit 1
fi
echo "TPC-H text data generation complete."

echo "moving from HDFS to GCS"
hadoop distcp hdfs://${DIR}/${SCALE}/customer ${BUCKET}/text/${SCALE}/customer
hadoop distcp hdfs://${DIR}/${SCALE}/lineitem ${BUCKET}/text/${SCALE}/lineitem
hadoop distcp hdfs://${DIR}/${SCALE}/nation ${BUCKET}/text/${SCALE}/nation
hadoop distcp hdfs://${DIR}/${SCALE}/orders ${BUCKET}/text/${SCALE}/orders
hadoop distcp hdfs://${DIR}/${SCALE}/part ${BUCKET}/text/${SCALE}/part
hadoop distcp hdfs://${DIR}/${SCALE}/partsupp ${BUCKET}/text/${SCALE}/partsupp
hadoop distcp hdfs://${DIR}/${SCALE}/region ${BUCKET}/text/${SCALE}/region
hadoop distcp hdfs://${DIR}/${SCALE}/supplier ${BUCKET}/text/${SCALE}/supplier

# Create the text/flat tables as external tables. These will be later be converted to Parquet.
echo "Loading text data into external and parquet tables"
beeline -u "jdbc:hive2://localhost:10000" -f /home/admin_jtaras_altostrat_com/tpch-generator/datagen/ddl/text/alltables2.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_parquet_${SCALE} --hivevar LOCATION=${BUCKET}/text/${SCALE}


if [ $DATA_FORMAT -eq "hudi" ]; then
	echo "Loading hudi data..."
	spark-sql -f ddl/hudi/hudiDDL.sql -hivevar DB=tpch_parquet_${SCALE} --hivevar DB1=tpch_hudi_${SCALE} \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
fi
