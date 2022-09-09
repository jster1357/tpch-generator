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

echo "DATA SCALE: $SCALE"
echo "BUCKET: $BUCKET"
echo "DATA FORMAT: $DATA_FORMAT"


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
echo "Loading data into seleted format..."

function load_text(){
	echo "Loading text data.."
 #beeline -u "jdbc:hive2://localhost:10000" -f ddl/text/textDDL.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_text_${SCALE} --hivevar LOCATION=${BUCKET}/text/${SCALE}
	spark-sql -f ddl/text/textDDL.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_text_${SCALE} --hivevar LOCATION=${BUCKET}/text/${SCALE}
}

function load_avro(){
	echo "Loading avro data..."
	spark-sql -f ddl/avro/avroDDL.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_avro_${SCALE}
}



function load_parquet(){
	echo "Loading parquet data..."
	#beeline -u "jdbc:hive2://localhost:10000" -f ddl/parquet/parquetDDL.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_parquet_${SCALE}
	spark-sql -f ddl/parquet/parquetDDL.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_parquet_${SCALE}
}

function load_hudi(){
	echo "Loading hudi data..."
	spark-sql -f ddl/hudi/hudiDDL.sql -hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_hudi_${SCALE} \
  --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.12.0 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
}

function load_iceberg(){
        echo "Loading iceberg data..."
        spark-sql -f ddl/iceberg/icebergDDL.sql -hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_iceberg_${SCALE} \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:0.14.0 \
    	--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    	--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog 
}

function load_delta(){
	echo "Loading delta data..."
	spark-sql -f /ddl/delta/deltaDDL.sql -hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_delta_${SCALE} \ 
	--packages io.delta:delta-core_2.12:1.0.1 \
	--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
	--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

function load_orc(){
	echo "Loading orc data..."
	beeline -u "jdbc:hive2://localhost:10000" -f ddl/orc/orcDDL.sql --hivevar DB=tpch_text_${SCALE} --hivevar DB1=tpch_orc_${SCALE} --hivevar LOCATION=${BUCKET}/text/${SCALE}
}

echo "loading text (default)..."
load_text

if [ "$DATA_FORMAT" = "hudi" ]; then
	load_hudi

elif [ "$DATA_FORMAT" = "delta" ]; then
	load_delta

elif [ "$DATA_FORMAT" = "orc" ]; then 
	load_orc

elif [ "$DATA_FORMAT" = "parquet" ]; then
	load_parquet

elif [ "$DATA_FORMAT" = "iceberg" ]; then 
	load_iceberg

elif [ "$DATA_FORMAT" = "avro" ]; then
	load_avro

elif [ "$DATA_FORMAT" = "all" ]; then
	load_hudi
	load_avro
	load_parquet
	load_delta
	load_iceberg
	load_orc
fi

echo "Finished Loading Files"
