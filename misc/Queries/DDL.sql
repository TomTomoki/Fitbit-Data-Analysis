create database FITBIT_Analysis;
create schema landing;
create schema staging;
create schema prod;

create database airflow_db;

create table LANDING.sleep (load_timestamp timestamp with time zone, load_json json);
create table LANDING.steps (load_timestamp timestamp with time zone, load_json json);
create table LANDING.calories (load_timestamp timestamp with time zone, load_json json);
create table LANDING.distance (load_timestamp timestamp with time zone, load_json json);
create table LANDING.sedentary (load_timestamp timestamp with time zone, load_json json);
create table LANDING.heartrate (load_timestamp timestamp with time zone, load_json json);


create table PROD.Google_Forms_Responses (
	load_timestamp timestamp with time zone
	, recorded_date date
	, motivated_level smallint
	, happiness_level smallint
	, tiredness_level smallint
	, breakfast_yday boolean
	, lunch_yday boolean
	, snack_amount_yday smallint
	, minutes_assignments_yday smallint
	, minutes_self_study_yday smallint
	, minutes_job_hunting_yday smallint
	, minutes_exercises_yday smallint
);