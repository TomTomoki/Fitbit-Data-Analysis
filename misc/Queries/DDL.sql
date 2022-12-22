create database FITBIT_Analysis;
create schema landing;

create table LANDING.sleep (load_timestamp timestamp with time zone, load_json json);
create table LANDING.steps (load_timestamp timestamp with time zone, load_json json);
create table LANDING.calories (load_timestamp timestamp with time zone, load_json json);
