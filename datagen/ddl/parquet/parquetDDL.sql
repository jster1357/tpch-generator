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
