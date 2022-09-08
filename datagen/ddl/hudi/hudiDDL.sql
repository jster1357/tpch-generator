create database if not exists ${DB1};
use ${DB1};

drop table if exists ${DB1}.customer;
drop table if exists ${DB1}.lineitem;
drop table if exists ${DB1}.nation;
drop table if exists ${DB1}.orders;
drop table if exists ${DB1}.part;
drop table if exists ${DB1}.partsupp;
drop table if exists ${DB1}.region;
drop table if exists ${DB1}.supplier;

create table ${DB1}.customer
using hudi
tblproperties (primaryKey = 'c_custkey')
as
select * from ${DB}.customer;

create table ${DB1}.lineitem
using hudi
tblproperties (primaryKey = 'l_orderkey','l_partkey','l_suppkey')
as 
select * from ${DB}.lineitem;

create table ${DB1}.nation
using hudi
tblproperties (primaryKey = 'n_nationkey')
as
select * from ${DB}.nation;

create table ${DB1}.orders
using hudi
tblproperties (primaryKey = 'o_orderkey','o_custkey')
as 
select * from ${DB}.orders;

create table ${DB1}.part
using hudi
tblproperties (primaryKey = 'p_partkey')
as 
select * from ${DB}.part;

create table ${DB1}.partsupp
using hudi
tblproperties (primaryKey = 'ps_partkey','ps_suppkey')
as 
select * from ${DB}.partsupp;

create table ${DB1}.region
using hudi
tblproperties (primaryKey = 'c_regionkey')
as 
select * from ${DB}.region;

create table ${DB1}.supplier
using hudi
tblproperties (primaryKey = 's_suppkey')
as 
select * from ${DB}.supplier;
