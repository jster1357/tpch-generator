create database if not exists ${DB};
use ${DB};

drop table if exists partsupp;

create table partsupp
stored as parquet
as select * from ${SOURCE}.partsupp;
