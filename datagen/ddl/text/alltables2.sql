create database if not exists ${DB};
use ${DB};

drop table if exists lineitem;
create external table lineitem 
(L_ORDERKEY BIGINT,
 L_PARTKEY BIGINT,
 L_SUPPKEY BIGINT,
 L_LINENUMBER INT,
 L_QUANTITY DOUBLE,
 L_EXTENDEDPRICE DOUBLE,
 L_DISCOUNT DOUBLE,
 L_TAX DOUBLE,
 L_RETURNFLAG STRING,
 L_LINESTATUS STRING,
 L_SHIPDATE STRING,
 L_COMMITDATE STRING,
 L_RECEIPTDATE STRING,
 L_SHIPINSTRUCT STRING,
 L_SHIPMODE STRING,
 L_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '${LOCATION}/lineitem';

drop table if exists part;
create external table part (P_PARTKEY BIGINT,
 P_NAME STRING,
 P_MFGR STRING,
 P_BRAND STRING,
 P_TYPE STRING,
 P_SIZE INT,
 P_CONTAINER STRING,
 P_RETAILPRICE DOUBLE,
 P_COMMENT STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '${LOCATION}/part/';

drop table if exists supplier;
create external table supplier (S_SUPPKEY BIGINT,
 S_NAME STRING,
 S_ADDRESS STRING,
 S_NATIONKEY BIGINT,
 S_PHONE STRING,
 S_ACCTBAL DOUBLE,
 S_COMMENT STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE 
LOCATION '${LOCATION}/supplier/';

drop table if exists partsupp;
create external table partsupp (PS_PARTKEY BIGINT,
 PS_SUPPKEY BIGINT,
 PS_AVAILQTY INT,
 PS_SUPPLYCOST DOUBLE,
 PS_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION'${LOCATION}/partsupp';

drop table if exists nation;
create external table nation (N_NATIONKEY BIGINT,
 N_NAME STRING,
 N_REGIONKEY BIGINT,
 N_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${LOCATION}/nation';

drop table if exists region;
create external table region (R_REGIONKEY BIGINT,
 R_NAME STRING,
 R_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${LOCATION}/region';

drop table if exists customer;
create external table customer (C_CUSTKEY BIGINT,
 C_NAME STRING,
 C_ADDRESS STRING,
 C_NATIONKEY BIGINT,
 C_PHONE STRING,
 C_ACCTBAL DOUBLE,
 C_MKTSEGMENT STRING,
 C_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${LOCATION}/customer';

drop table if exists orders;
create external table orders (O_ORDERKEY BIGINT,
 O_CUSTKEY BIGINT,
 O_ORDERSTATUS STRING,
 O_TOTALPRICE DOUBLE,
 O_ORDERDATE STRING,
 O_ORDERPRIORITY STRING,
 O_CLERK STRING,
 O_SHIPPRIORITY INT,
 O_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '${LOCATION}/orders';



create database if not exists ${DB1};
use ${DB1};

drop table if exists ${DB1}.customer;
create table ${DB1}.customer
stored as parquet
as select * from ${DB}.customer;

drop table if exists ${DB1}.lineitem;
create table ${DB1}.lineitem
stored as parquet
as select * from ${DB}.lineitem;

drop table if exists ${DB1}.nation;
create table ${DB1}.nation
stored as parquet
as select * from ${DB}.nation;

drop table if exists ${DB1}.orders;
create table ${DB1}.orders
stored as parquet
as select * from ${DB}.orders;

drop table if exists ${DB1}.part;
create table ${DB1}.part
stored as parquet
as select * from ${DB}.part;

drop table if exists ${DB1}.partsupp;
create table ${DB1}.partsupp
stored as parquet
as select * from ${DB}.partsupp;

drop table if exists ${DB1}.region;
create table ${DB1}.region
stored as parquet
as select * from ${DB}.region;

drop table if exists ${DB1}.supplier;
create table ${DB1}.supplier
stored as parquet
as select * from ${DB}.supplier;

analyze table ${DB1}.customer compute statistics for columns;
analyze table ${DB1}.lineitem compute statistics for columns;
analyze table ${DB1}.nation compute statistics for columns;
analyze table ${DB1}.orders compute statistics for columns;
analyze table ${DB1}.part compute statistics for columns;
analyze table ${DB1}.partsupp compute statistics for columns;
analyze table ${DB1}.region compute statistics for columns;
analyze table ${DB1}.supplier compute statistics for columns;