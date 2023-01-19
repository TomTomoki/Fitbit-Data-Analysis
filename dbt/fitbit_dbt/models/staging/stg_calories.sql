with expanded as (
	select
		load_json -> 'activities-calories' -> 0 ->> 'dateTime' as recorded_date
		, load_json -> 'activities-calories' -> 0 ->> 'value' as total_calories
		, json_array_elements(load_json -> 'activities-calories-intraday' -> 'dataset') as calories_detail
		, load_timestamp
	from landing.calories
	{% if is_incremental() %}

		where to_timestamp(load_json -> 'activities-calories' -> 0 ->> 'dateTime', 'YYYY-MM-DD') > (select COALESCE(max(date(recorded_time)), '2022-09-30') from {{ this }})

	{% endif %}
)
, json_parsed as (
	select
		recorded_date
		, total_calories
		, cast(calories_detail ->> 'time' as time) as recorded_time
		, cast(calories_detail ->> 'mets' as smallint) as mets
		, cast(calories_detail ->> 'value' as decimal(10, 3)) as calories
		, load_timestamp
	from expanded
)
, hourly_interval_converted as (
	select
		recorded_date::date + recorded_time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time)) + 3600) / 3600) * 3600) as hourly_interval
		, mets
		, calories
		, load_timestamp
	from json_parsed
)
select
	hourly_interval as recorded_time
	, round(avg(mets), 2) as avg_mets
	, round(sum(calories), 2) as sum_calories
	, load_timestamp
from hourly_interval_converted
group by hourly_interval, load_timestamp
order by hourly_interval