from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
import pyspark
import sys

spark = SparkSession .builder .master('yarn')  .appName('hudi tpch loader') .getOrCreate()

scale = str(sys.argv[1])

target_db='tpch_iceberg_' + str(scale)
source_db='tpch_parquet_' + str(scale)
spark.sql("create database if not exists %s" % target_db)

spark.sql("drop table if exists %s.customer" % target_db)
spark.sql("drop table if exists %s.lineitem" % target_db)
spark.sql("drop table if exists %s.nation" % target_db)
spark.sql("drop table if exists %s.orders" % target_db)
spark.sql("drop table if exists %s.part" % target_db)
spark.sql("drop table if exists %s.partsupp" % target_db)
spark.sql("drop table if exists %s.region" % target_db)
spark.sql("drop table if exists %s.supplier" % target_db)


spark.sql("create table %s.customer using iceberg as select * from %s.customer" % (target_db, source_db))
spark.sql("create table %s.lineitem using iceberg as select * from %s.lineitem" % (target_db, source_db))
spark.sql("create table %s.nation using iceberg as select * from %s.nation" % (target_db, source_db))
spark.sql("create table %s.orders using iceberg as select * from %s.orders" % (target_db, source_db))
spark.sql("create table %s.part using iceberg as select * from %s.part" % (target_db, source_db))
spark.sql("create table %s.partsupp using iceberg as select * from %s.partsupp" % (target_db, source_db))
spark.sql("create table %s.region using iceberg as select * from %s.region" % (target_db, source_db))
spark.sql("create table %s.supplier using iceberg as select * from %s.supplier" % (target_db, source_db))
