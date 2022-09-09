create database if not exists ${DB};
use ${DB};

SET spark.hadoop.mapred.input.dir.recursive=true;
SET spark.hadoop.hive.mapred.supports.subdirectories=true;
SET spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true;

drop table if exists ${DB1}.customer;
drop table if exists ${DB1}.lineitem;
drop table if exists ${DB1}.nation;
drop table if exists ${DB1}.orders;
drop table if exists ${DB1}.part;
drop table if exists ${DB1}.partsupp;
drop table if exists ${DB1}.region;
drop table if exists ${DB1}.supplier;

drop table if exists ${DB1}.customer_external;
drop table if exists ${DB1}.lineitem_external;
drop table if exists ${DB1}.nation_external;
drop table if exists ${DB1}.orders_external;
drop table if exists ${DB1}.part_external;
drop table if exists ${DB1}.partsupp_external;
drop table if exists ${DB1}.region_external;
drop table if exists ${DB1}.supplier_external;

create table ${DB1}.lineitem_external 
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
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/lineitem';

create table ${DB1}.part_external (
P_PARTKEY BIGINT,
P_NAME STRING,
P_MFGR STRING,
P_BRAND STRING,
P_TYPE STRING,
P_SIZE INT,
P_CONTAINER STRING,
P_RETAILPRICE DOUBLE,
P_COMMENT STRING) 
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/part/';

create table ${DB1}.supplier_external (
S_SUPPKEY BIGINT,
S_NAME STRING,
S_ADDRESS STRING,
S_NATIONKEY BIGINT,
S_PHONE STRING,
S_ACCTBAL DOUBLE,
S_COMMENT STRING) 
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/supplier/';

create table ${DB1}.partsupp_external (
PS_PARTKEY BIGINT,
PS_SUPPKEY BIGINT,
PS_AVAILQTY INT,
PS_SUPPLYCOST DOUBLE,
PS_COMMENT STRING)
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION'${LOCATION}/partsupp';

create table ${DB1}.nation_external (
N_NATIONKEY BIGINT,
N_NAME STRING,
N_REGIONKEY BIGINT,
N_COMMENT STRING)
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/nation';

create table ${DB1}.region_external (
R_REGIONKEY BIGINT,
R_NAME STRING,
R_COMMENT STRING)
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/region';

create table ${DB1}.customer_external (
C_CUSTKEY BIGINT,
C_NAME STRING,
C_ADDRESS STRING,
C_NATIONKEY BIGINT,
C_PHONE STRING,
C_ACCTBAL DOUBLE,
C_MKTSEGMENT STRING,
C_COMMENT STRING)
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/customer';

create table ${DB1}.orders_external (
O_ORDERKEY BIGINT,
O_CUSTKEY BIGINT,
O_ORDERSTATUS STRING,
O_TOTALPRICE DOUBLE,
O_ORDERDATE STRING,
O_ORDERPRIORITY STRING,
O_CLERK STRING,
O_SHIPPRIORITY INT,
O_COMMENT STRING)
USING CSV
OPTIONS ("delimiter" ='|')
LOCATION '${LOCATION}/orders';

create table ${DB1}.customer
using CSV
as
select * from ${DB1}.customer_external;


create table ${DB1}.lineitem
using CSV
as 
select * from ${DB1}.lineitem_external;


create table ${DB1}.nation
using CSV
as
select * from ${DB1}.nation_external;


create table ${DB1}.orders
using CSV
as 
select * from ${DB1}.orders_external;


create table ${DB1}.part
using CSV
as 
select * from ${DB1}.part_external;


create table ${DB1}.partsupp
using CSV
as 
select * from ${DB1}.partsupp_external;

create table ${DB1}.region
using CSV
as 
select * from ${DB1}.region_external;

create table ${DB1}.supplier
using CSV
as 
select * from ${DB1}.supplier_external;
