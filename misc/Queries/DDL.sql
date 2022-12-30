create database FITBIT_Analysis;
create schema landing;
create schema staging;
create schema prod;


create table LANDING.sleep (load_timestamp timestamp with time zone, load_json json);
create table LANDING.steps (load_timestamp timestamp with time zone, load_json json);
create table LANDING.calories (load_timestamp timestamp with time zone, load_json json);
create table LANDING.distance (load_timestamp timestamp with time zone, load_json json);
create table LANDING.sedentary (load_timestamp timestamp with time zone, load_json json);
create table LANDING.heartrate (load_timestamp timestamp with time zone, load_json json);