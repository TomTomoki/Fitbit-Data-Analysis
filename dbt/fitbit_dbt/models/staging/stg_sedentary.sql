with expanded as (
	select
		load_json -> 'activities-minutesSedentary' -> 0 #>> '{dateTime}' as recorded_date
		, load_json -> 'activities-minutesSedentary' -> 0 #>> '{value}' as daily_sedentary
		, json_array_elements(load_json -> 'activities-minutesSedentary-intraday' #> '{dataset}') as sedentary_detail
		, load_timestamp
	from LANDING.sedentary
)
, json_parsed as (
	select
		recorded_date
		, daily_sedentary
		, sedentary_detail ->> 'time' as recorded_time
		, sedentary_detail ->> 'value' as sedentary
		, load_timestamp
	from expanded
)
, min30_interval_converted as (
	select
		recorded_date
		, recorded_date::date + recorded_time::time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time::time)) + 1800) / 1800) * 1800) as min30_interval
		, sedentary::numeric
		, load_timestamp
	from json_parsed
)
select
	recorded_date
	, min30_interval as recorded_time
	, sum(sedentary) as sedentary
	, load_timestamp
from min30_interval_converted
group by recorded_date, min30_interval, load_timestamp
order by recorded_time