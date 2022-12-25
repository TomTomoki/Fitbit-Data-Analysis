with expanded as (
	select
		load_json -> 'activities-steps' -> 0 ->> 'dateTime' as recorded_date
		, load_json -> 'activities-steps' -> 0 ->> 'value' as total_steps
		, json_array_elements(load_json -> 'activities-steps-intraday' -> 'dataset') as steps_detail
		, load_timestamp
	from landing.steps
	{% if is_incremental() %}

		where load_json -> 'activities-steps' -> 0 ->> 'dateTime' > (select max(recorded_date) from {{ this }})

	{% endif %}
)
, json_parsed as (
	select
		recorded_date
		, total_steps
		, cast(steps_detail ->> 'time' as time) as recorded_time
		, cast(steps_detail ->> 'value' as integer) as steps
		, load_timestamp
	from expanded
)
, min30_interval_converted as (
	select
		recorded_date
		, recorded_date::date + recorded_time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time)) + 1800) / 1800) * 1800) as min30_interval
		, steps
		, load_timestamp
	from json_parsed
)
select
	recorded_date
	, min30_interval as recorded_time
	, sum(steps) as num_steps
	, load_timestamp
from min30_interval_converted
group by recorded_date, min30_interval, load_timestamp
order by min30_interval