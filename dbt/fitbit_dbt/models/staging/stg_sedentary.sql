with expanded as (
	select
		load_json -> 'activities-minutesSedentary' -> 0 #>> '{dateTime}' as recorded_date
		, load_json -> 'activities-minutesSedentary' -> 0 #>> '{value}' as daily_sedentary
		, json_array_elements(load_json -> 'activities-minutesSedentary-intraday' #> '{dataset}') as sedentary_detail
		, load_timestamp
	from landing.sedentary
	{% if is_incremental() %}

		where to_timestamp(load_json -> 'activities-minutesSedentary' -> 0 ->> 'dateTime', 'YYYY-MM-DD') > (select COALESCE(max(date(recorded_time)), '2022-09-30') from {{ this }})

	{% endif %}
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
, hourly_interval_converted as (
	select
		recorded_date
		, recorded_date::date + recorded_time::time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time::time)) + 3600) / 3600) * 3600) as hourly_interval
		, sedentary::numeric
		, load_timestamp
	from json_parsed
)
select
	hourly_interval as recorded_time
	, sum(sedentary) as sedentary
	, load_timestamp
from hourly_interval_converted
group by hourly_interval, load_timestamp
order by recorded_time