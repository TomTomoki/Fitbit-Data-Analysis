with expanded as (
	select
		load_json -> 'activities-calories' -> 0 ->> 'dateTime' as recorded_date
		, load_json -> 'activities-calories' -> 0 ->> 'value' as total_calories
		, json_array_elements(load_json -> 'activities-calories-intraday' -> 'dataset') as calories_detail
	from landing.calories
	{% if is_incremental() %}

		where load_json -> 'activities-calories' -> 0 ->> 'dateTime' > (select max(recorded_date) from {{ this }})

	{% endif %}
)
, json_parsed as (
	select
		recorded_date
		, total_calories
		, cast(calories_detail ->> 'time' as time) as recorded_time
		, cast(calories_detail ->> 'mets' as smallint) as mets
		, cast(calories_detail ->> 'value' as decimal(10, 3)) as calories
	from expanded
)
, min30_interval_converted as (
	select
		recorded_date
		, recorded_date::date + recorded_time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time)) + 1800) / 1800) * 1800) as min30_interval
		, mets
		, calories
	from json_parsed
)
select
	recorded_date
	, min30_interval as recorded_time
	, round(avg(mets), 2) as avg_mets
	, round(sum(calories), 2) as sum_calories
from min30_interval_converted
group by recorded_date, min30_interval
order by min30_interval