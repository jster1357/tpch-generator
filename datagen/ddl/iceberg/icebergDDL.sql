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
using iceberg
as
select * from ${DB}.customer;

create table ${DB1}.lineitem
using iceberg
as 
select * from ${DB}.lineitem;

create table ${DB1}.nation
using iceberg
as
select * from ${DB}.nation;

create table ${DB1}.orders
using iceberg
as 
select * from ${DB}.orders;

create table ${DB1}.part
using iceberg
as 
select * from ${DB}.part;

create table ${DB1}.partsupp
using iceberg
as 
select * from ${DB}.partsupp;

create table ${DB1}.region
using iceberg
as 
select * from ${DB}.region;

create table ${DB1}.supplier
using iceberg
as 
select * from ${DB}.supplier;
