create database if not exists ${DB1};
use ${DB1};

drop table if exists ${DB1}.customer;
create table ${DB1}.customer
using hudi
tblproperties (primaryKey = 'c_custkey')
as
select * from ${DB}.customer;

drop table if exists ${DB1}.lineitem;
create table ${DB1}.lineitem
tblproperties (primaryKey = 'l_orderkey','l_partkey','l_suppkey')
using hudi
as 
select * from ${DB}.lineitem;

drop table if exists ${DB1}.nation;
create table ${DB1}.nation
tblproperties (primaryKey = 'n_nationkey')
using hudi
as
select * from ${DB}.nation;

drop table if exists ${DB1}.orders;
create table ${DB1}.orders
tblproperties (primaryKey = 'o_orderkey','o_custkey')
using hudi
as 
select * from ${DB}.orders;

drop table if exists ${DB1}.part;
create table ${DB1}.part
tblproperties (primaryKey = 'p_partkey')
using hudi
as 
select * from ${DB}.part;

drop table if exists ${DB1}.partsupp;
create table ${DB1}.partsupp
tblproperties (primaryKey = 'ps_partkey','ps_suppkey')
using hudi
as 
select * from ${DB}.partsupp;

drop table if exists ${DB1}.region;
create table ${DB1}.region
tblproperties (primaryKey = 'c_regionkey')
using hudi
as 
select * from ${DB}.region;

drop table if exists ${DB1}.supplier;
create table ${DB1}.supplier
tblproperties (primaryKey = 's_suppkey')
using hudi
as 
select * from ${DB}.supplier;

