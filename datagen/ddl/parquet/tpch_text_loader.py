#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
import pyspark
import sys

spark = SparkSession .builder .master('yarn')  .appName('jtaras') .getOrCreate()

gcs_bucket = str(sys.argv[1])
scale = str(sys.argv[2])


# In[ ]:


db='tpch_parquet_' + str(scale)
spark.sql("create database if not exists %s" % db)


gcs_read_path = gcs_bucket + '/text/' + scale + '/customer'
table_name = 'tpch_parquet_' + scale +'.customer'

schema = StructType([     StructField("c_custkey", IntegerType(), True),    StructField("c_name", StringType(), True),    StructField("c_address", StringType(), True),    StructField("c_nationkey", IntegerType(), True),    StructField("c_phone", StringType(), True),    StructField("c_acctbal", DoubleType(), True),    StructField("c_mktsegment", StringType(), True),    StructField("c_comment", StringType(), True),    StructField("c_null", StringType(), True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[9]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/lineitem'
table_name = 'tpch_parquet_' + scale +'.lineitem'

schema = StructType([     StructField("l_orderkey",IntegerType(),True),     StructField("l_partkey",IntegerType(),True),     StructField("l_suppkey",IntegerType(),True),     StructField("l_linenumber",IntegerType(),True),     StructField("l_quantity",DoubleType(),True),     StructField("l_extendedprice",DoubleType(),True),     StructField("l_discount",DoubleType(),True),     StructField("l_tax",DoubleType(),True),     StructField("l_returnflag",StringType(),True),     StructField("l_linestatus",StringType(),True),     StructField("l_shipdate",StringType(),True),     StructField("l_commitdate",StringType(),True),     StructField("l_receiptdate",StringType(),True),     StructField("l_shipinstruct",StringType(),True),     StructField("l_shipmode",StringType(),True),     StructField("l_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[7]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/orders'
table_name = 'tpch_parquet_' + scale +'.orders'

schema = StructType([     StructField("o_orderkey",StringType(),True),     StructField("o_custkey",IntegerType(),True),     StructField("o_orderstatus",StringType(),True),     StructField("o_totalprice",DoubleType(),True),     StructField("o_orderdate",StringType(),True),     StructField("o_orderpriority",StringType(),True),     StructField("o_clerk",StringType(),True),     StructField("o_shippriority",IntegerType(),True),     StructField("o_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[10]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/part'
table_name = 'tpch_parquet_' + scale +'.part'

schema = StructType([     StructField("p_partkey",IntegerType(),True),     StructField("p_name",StringType(),True),     StructField("p_mfgr",StringType(),True),     StructField("p_brand",StringType(),True),     StructField("p_type",StringType(),True),     StructField("p_size",IntegerType(),True),     StructField("p_container",StringType(),True),     StructField("p_retailprice",DoubleType(),True),     StructField("p_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[11]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/supplier'
table_name = 'tpch_parquet_' + scale +'.supplier'

schema = StructType([     StructField("s_suppkey",IntegerType(),True),     StructField("s_name",StringType(),True),     StructField("s_address",StringType(),True),     StructField("s_nationkey",IntegerType(),True),     StructField("s_phone",StringType(),True),     StructField("s_acctbal",DoubleType(),True),     StructField("s_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[12]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/partsupp'
table_name = 'tpch_parquet_' + scale +'.partsupp'

schema = StructType([     StructField("ps_partkey",IntegerType(),True),     StructField("ps_suppkey",IntegerType(),True),     StructField("ps_availqty",IntegerType(),True),     StructField("ps_supplycost",DoubleType(),True),     StructField("ps_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[14]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/nation'
table_name = 'tpch_parquet_' + scale +'.nation'

schema = StructType([     StructField("n_nationkey",IntegerType(),True),     StructField("n_name",StringType(),True),     StructField("n_regionkey",IntegerType(),True),     StructField("n_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


# In[15]:


gcs_read_path = gcs_bucket + '/text/' + scale + '/region'
table_name = 'tpch_parquet_' + scale +'.region'

schema = StructType([     StructField("r_regionkey",IntegerType(),True),     StructField("r_name",StringType(),True),     StructField("r_comment",StringType(),True)])

df = spark.read.csv(gcs_read_path, sep="|", schema=schema)
df.write.mode("overwrite").saveAsTable(table_name)


