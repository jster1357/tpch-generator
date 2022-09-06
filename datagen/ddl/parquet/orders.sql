create database if not exists ${DB};
use ${DB};

drop table if exists orders;

create table ordersI
stored as parquet
as select * from ${SOURCE}.orders;
