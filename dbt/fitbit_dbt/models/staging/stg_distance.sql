with expanded as (
	select
		load_json -> 'activities-distance' -> 0 #>> '{dateTime}' as recorded_date
		, load_json -> 'activities-distance' -> 0 #>> '{value}' as daily_distance
		, json_array_elements(load_json -> 'activities-distance-intraday' #> '{dataset}') as distance_detail
		, load_timestamp
	from landing.distance
)
, json_parsed as (
	select
		recorded_date
		, daily_distance
		, distance_detail ->> 'time' as recorded_time
		, distance_detail ->> 'value' as distance
		, load_timestamp
	from expanded
)
, hourly_interval_converted as (
	select
		recorded_date::date + recorded_time::time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time::time)) + 3600) / 3600) * 3600) as hourly_interval
		, distance::numeric
		, load_timestamp
	from json_parsed
)
select
	hourly_interval as recorded_time
	, round(sum(distance) * 1000, 2) as distance_meters
	, load_timestamp
from hourly_interval_converted
group by hourly_interval, load_timestamp
order by recorded_time