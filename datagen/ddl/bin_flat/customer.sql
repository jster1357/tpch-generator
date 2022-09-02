create database if not exists ${DB};
use ${DB};

drop table if exists customer;

create table customer
stored as parquet
as select * from ${SOURCE}.customer;
