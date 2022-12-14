with expanded as (
	select
		load_json -> 'activities-steps' -> 0 ->> 'dateTime' as recorded_date
		, load_json -> 'activities-steps' -> 0 ->> 'value' as total_steps
		, json_array_elements(load_json -> 'activities-steps-intraday' -> 'dataset') as steps_detail
		, load_timestamp
	from landing.steps
	{% if is_incremental() %}

		where load_json -> 'activities-steps' -> 0 ->> 'dateTime' > (select COALESCE(max(recorded_date), '2022-09-30') from {{ this }})

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
, hourly_interval_converted as (
	select
		recorded_date
		, recorded_date::date + recorded_time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time)) + 3600) / 3600) * 3600) as hourly_interval
		, steps
		, load_timestamp
	from json_parsed
)
select
	recorded_date
	, hourly_interval as recorded_time
	, sum(steps) as num_steps
	, load_timestamp
from hourly_interval_converted
group by recorded_date, hourly_interval, load_timestamp
order by hourly_interval