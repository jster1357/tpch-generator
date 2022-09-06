create database if not exists ${DB};
use ${DB};

drop table if exists part;

create table part
stored as parquet
as select * from ${SOURCE}.part;
