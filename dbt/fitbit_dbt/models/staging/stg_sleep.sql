with sleep_data as (
	select
		load_json #> '{sleep,0}' ->> 'logId' as logId
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'dateTime' as dateTime
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'level' as level
		, cast(json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'seconds' as integer) as seconds
		, load_timestamp
	from landing.sleep
	{% if is_incremental() %}

		where load_timestamp > (select COALESCE(max(load_timestamp), '2022-09-30') from {{ this }})

	{% endif %}
),
sleep_shortData as (
	select
		load_json #> '{sleep,0}' ->> 'logId' as logId
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'dateTime' as dateTime
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'level' as level
		, cast(json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'seconds' as integer) as seconds
		, load_timestamp
	from landing.sleep
	{% if is_incremental() %}

		where load_timestamp > (select COALESCE(max(load_timestamp), '2022-09-30') from {{ this }})

	{% endif %}
),
add_endTime_data as(
	select
		logId
		, level
		, cast(dateTime as timestamp) as level_start
		, cast(dateTime as timestamp) + interval '1 second' * seconds as level_end
		, seconds
		, load_timestamp
	from sleep_data
	{% if is_incremental() %}

		where cast(dateTime as timestamp) > (select COALESCE(max(end_time), '2022-09-30') from {{ this }})

	{% endif %}
),
add_endTime_shortData as(
	select
		logId
		, level
		, cast(dateTime as timestamp) as level_start
		, cast(dateTime as timestamp) + interval '1 second' * seconds as level_end
		, seconds
		, load_timestamp
	from sleep_shortData
	{% if is_incremental() %}

		where cast(dateTime as timestamp) > (select COALESCE(max(end_time), '2022-09-30') from {{ this }})

	{% endif %}
),
overlapped_data as (
	select
		a.logId
		, a.level as a_level
		, a.level_start as a_start
		, a.level_end as a_end
		, a.seconds as a_seconds
		, b.level as b_level
		, b.level_start as b_start
		, b.level_end b_end
		, b.seconds as b_seconds
		, a.load_timestamp
	from add_endTime_data a
	left join add_endTime_shortData b
		on a.logId = b.logId
		and  b.level_start >= a.level_start and b.level_end <= a.level_end
),
overlapped_data_with_non_null_shortData as (
	select
		*
	from overlapped_data
	where b_level is not null
),
break_down_data_by_shortData_before as (
	select
		logId
		, a_level as level
		, case
			when (a_start != lag(a_start, 1) over (order by a_start, a_end, b_start, b_end))
				or (lag(a_start, 1) over (order by a_start, a_end, b_start, b_end) is null)
				then a_start
			when a_start = lag(a_start, 1) over (order by a_start, a_end, b_start, b_end)
				then lag(b_end, 1) over (order by a_start, a_end, b_start, b_end)
		end as start_time
		, case
			when (a_start != lag(a_start, 1) over (order by a_start, a_end, b_start, b_end))
				or (lag(a_start, 1) over (order by a_start, a_end, b_start, b_end) is null)
				then b_start
			when a_start = lag(a_start, 1) over (order by a_start, a_end, b_start, b_end)
				then b_start
		end as end_time
		, load_timestamp
	from overlapped_data_with_non_null_shortData
),
break_down_data_by_shortData_after as (
	select
		logId
		, a_level as level
		, case
			when (a_start != lead(a_start, 1) over (order by a_start, a_end, b_start, b_end))
				or (lead(a_start, 1) over (order by a_start, a_end, b_start, b_end) is null)
				then b_end
		end as start_time
		, a_end as end_time
		, load_timestamp
	from overlapped_data_with_non_null_shortData
),
final_sleep_data as (
	select
		*
	from break_down_data_by_shortData_before
	where start_time != end_time
	union all
	select
		*
	from break_down_data_by_shortData_after
	where start_time is not null
		and start_time != end_time
	union all
	select
		logId
		, a_level as level
		, a_start as start_time
		, a_end as end_time
		, load_timestamp
	from overlapped_data
	where b_level is null
	union all
	select 
		logId
		, b_level as level
		, b_start as start_time
		, b_end as end_time
		, load_timestamp
	from overlapped_data
	where b_level is not null
)
select 
	logId
	, level
	, cast(start_time as TIMESTAMP WITH TIME ZONE) as start_time
	, cast(end_time as TIMESTAMP WITH TIME ZONE) as end_time
	, load_timestamp
from final_sleep_data