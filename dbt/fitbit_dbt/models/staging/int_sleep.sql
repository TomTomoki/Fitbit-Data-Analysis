with add_hourly_start_end as (
	select 
		date_trunc('hour', start_time) + '1 hour'::interval as start_date_hour
		, date_trunc('hour', end_time) + '1 hour'::interval as end_date_hour
		, level
		, case
			when level = 'light' then extract(epoch from (end_time - start_time))
			else 0
		end as level_light
		, case
			when level = 'rem' then extract(epoch from (end_time - start_time))
			else 0
		end as level_rem
		, case
			when level = 'deep' then extract(epoch from (end_time - start_time))
			else 0
		end as level_deep
		, case
			when level = 'wake' then extract(epoch from (end_time - start_time))
			else 0
		end as level_wake
		, start_time
		, end_time
		, logId
		, load_timestamp
	from {{ ref('stg_sleep') }}
    {% if is_incremental() %}

		where start_time > (select COALESCE(max(recorded_time), '2022-09-30') from {{ this }})

	{% endif %}
),
start_end_in_same_hour as (
	select 
		*
	from add_hourly_start_end
	where start_date_hour = end_date_hour
),
start_end_in_different_hour as (
	select 
		*
	from add_hourly_start_end
	where start_date_hour != end_date_hour
),
start_end_in_different_hour_before_recorded_time as (
	select 
		start_date_hour as recorded_time
		, case
			when level = 'light' then extract(epoch from (start_date_hour - start_time))
			else 0
		end as level_light
		, case
			when level = 'rem' then extract(epoch from (start_date_hour - start_time))
			else 0
		end as level_rem
		, case
			when level = 'deep' then extract(epoch from (start_date_hour - start_time))
			else 0
		end as level_deep
		, case
			when level = 'wake' then extract(epoch from (start_date_hour - start_time))
			else 0
		end as level_wake
		, logId
		, load_timestamp
	from start_end_in_different_hour
),
start_end_in_different_hour_after_recorded_time as (
	select 
		end_date_hour as recorded_time
		, case
			when level = 'light' then extract(epoch from (end_time - start_date_hour))
			else 0
		end as level_light
		, case
			when level = 'rem' then extract(epoch from (end_time - start_date_hour))
			else 0
		end as level_rem
		, case
			when level = 'deep' then extract(epoch from (end_time - start_date_hour))
			else 0
		end as level_deep
		, case
			when level = 'wake' then extract(epoch from (end_time - start_date_hour))
			else 0
		end as level_wake
		, logId
		, load_timestamp
	from start_end_in_different_hour
),
union_all_records as (
	select
		start_date_hour as recorded_time
		, level_light
		, level_rem
		, level_deep
		, level_wake
		, logId
		, load_timestamp
	from start_end_in_same_hour
	union all
	select
		recorded_time
		, level_light
		, level_rem
		, level_deep
		, level_wake
		, logId
		, load_timestamp
	from start_end_in_different_hour_before_recorded_time
	union all
	select
		recorded_time
		, level_light
		, level_rem
		, level_deep
		, level_wake
		, logId
		, load_timestamp
	from start_end_in_different_hour_after_recorded_time
),
final_aggregation as (
	select
		recorded_time
		, sum(level_light) as level_light
		, sum(level_rem) as level_rem
		, sum(level_deep) as level_deep
		, sum(level_wake) as level_wake
		, logId
		, load_timestamp
	from union_all_records
	group by recorded_time, logId, load_timestamp
)
select * from final_aggregation