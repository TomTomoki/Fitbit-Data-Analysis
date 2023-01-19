with expanded as (
	select
		load_json -> 'activities-heart' -> 0 #>> '{dateTime}' as recorded_date
		, json_array_elements(load_json -> 'activities-heart-intraday' #> '{dataset}') as heartrate_detail
		, load_timestamp
	from landing.heartrate
	{% if is_incremental() %}

		where cast(load_json -> 'activities-heart' -> 0 ->> 'dateTime' as date) > (select COALESCE(max(date(recorded_time)), '2022-09-30') from {{ this }})

	{% endif %}
)
, json_parsed as (
	select
		recorded_date
		, heartrate_detail ->> 'time' as recorded_time
		, heartrate_detail ->> 'value' as heartrate
		, load_timestamp
	from expanded
)
, hourly_interval_converted as (
	select
		recorded_date
		, recorded_date::date + recorded_time::time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time::time)) + 3600) / 3600) * 3600) as hourly_interval
		, heartrate::numeric
		, load_timestamp
	from json_parsed
)
select
	hourly_interval as recorded_time
	, round(avg(heartrate), 2) as avg_heartrate
	, max(heartrate) as max_heartrate
	, min(heartrate) as min_heartrate
	, load_timestamp
from hourly_interval_converted
group by hourly_interval, load_timestamp
order by recorded_time