create database if not exists ${DB};
use ${DB};

drop table if exists nation;

create table nation
stored as parquet
as select * from ${SOURCE}.nation;
