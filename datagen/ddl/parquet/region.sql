create database if not exists ${DB};
use ${DB};

drop table if exists region;

create table region
stored as parquet
as select * from ${SOURCE}.region;
