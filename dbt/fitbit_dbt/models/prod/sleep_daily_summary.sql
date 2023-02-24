with data_type_converted as (
	select
		dateofsleep as recorded_date
		, cast(duration as integer) / 60000 as duration
		, cast(efficiency as smallint) as efficiency
		, cast(endtime as timestamp with time zone) as sleep_end_time
		, cast(ismainsleep as bool) as is_main_sleep 
		, cast(minutesafterwakeup as smallint) as minutes_after_wakeup
		, cast(minutesasleep as smallint) as minutes_asleep
		, cast(minutesawake as smallint) as minutes_awake
		, cast(minutestofallasleep as smallint) as minutes_to_fall_asleep
		, cast(starttime as timestamp with time zone) as sleep_start_time
		, cast(timeinbed as smallint) as minutes_in_bed
		, cast(totalminutesasleep as smallint) as total_minutes_asleep
		, cast(totalsleeprecords as smallint) as total_sleep_records
		, cast(totaltimeinbed as smallint) as total_time_in_bed
    from {{ ref('stg_sleep_daily_summary') }}
	{% if is_incremental() %}

		where dateofsleep > (select COALESCE(max(recorded_date), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	*
from data_type_converted