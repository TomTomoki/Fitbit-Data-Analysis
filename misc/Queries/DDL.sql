create database FITBIT_Analysis;
create schema landing;

create table LANDING.sleep (load_date date, load_json json);
create table LANDING.steps (load_date date, load_json json);


create table LANDING.calories (load_date date, load_json json);