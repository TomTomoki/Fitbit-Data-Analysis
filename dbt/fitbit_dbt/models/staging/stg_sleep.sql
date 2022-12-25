with cte_sleep_overview as (
	select
		load_json #> '{sleep,0}' ->> 'dateOfSleep' as dateOfSleep
		, load_json #> '{sleep,0}' ->> 'duration' as duration
		, load_json #> '{sleep,0}' ->> 'efficiency' as efficiency
		, load_json #> '{sleep,0}' ->> 'endTime' as endTime
		, load_json #> '{sleep,0}' ->> 'isMainSleep' as isMainSleep
		, load_json #> '{sleep,0}' ->> 'logId' as logId
		, load_json #> '{sleep,0}' ->> 'minutesAfterWakeup' as minutesAfterWakeup
		, load_json #> '{sleep,0}' ->> 'minutesAsleep' as minutesAsleep
		, load_json #> '{sleep,0}' ->> 'minutesAwake' as minutesAwake
		, load_json #> '{sleep,0}' ->> 'minutesToFallAsleep' as minutesToFallAsleep
		, load_json #> '{sleep,0}' ->> 'startTime' as startTime
		, load_json #> '{sleep,0}' ->> 'timeInBed' as timeInBed
		, load_json -> 'summary' -> 'totalMinutesAsleep' as totalMinutesAsleep
		, load_json -> 'summary' -> 'totalSleepRecords' as totalSleepRecords
		, load_json -> 'summary' -> 'totalTimeInBed' as totalTimeInBed
		, load_timestamp
	from landing.sleep
	{% if is_incremental() %}

		where load_json #> '{sleep,0}' ->> 'dateOfSleep' > (select max(dateOfSleep) from {{ this }})

	{% endif %}
),
cte_union_shortData_data as (
	select
		load_json #> '{sleep,0}' ->> 'logId' as logId
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'dateTime' as dateTime
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'level' as level
		, cast(json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'seconds' as integer) as seconds
	from landing.sleep
	{% if is_incremental() %}

		where load_json #> '{sleep,0}' ->> 'dateOfSleep' > (select max(dateOfSleep) from {{ this }})

	{% endif %}
	union
	select
		load_json #> '{sleep,0}' ->> 'logId' as logId
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'dateTime' as dateTime
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'level' as level
		, cast(json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'seconds' as integer) as seconds
	from landing.sleep
	{% if is_incremental() %}

		where load_json #> '{sleep,0}' ->> 'dateOfSleep' > (select max(dateOfSleep) from {{ this }})

	{% endif %}
),
cte_add_endTime as(
	select
		logId
		, level
		, cast(datetime as timestamp) as level_start
		, cast(datetime as timestamp) + interval '1 second' * seconds as level_end
		, seconds
	from cte_union_shortData_data
),
cte_overlap_sleep_level as (
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
		, rank() over (partition by a.level, a.level_start, a.level_end order by b.level_start desc) as ranking
	from cte_add_endTime a
	left join cte_add_endTime b on b.level_start > a.level_start and b.level_start < a.level_end
),
cte_aggregated_at_sleep_level_interval as (
	select
		logId
		, a_level
		, a_start
		, a_end
		, max(a_seconds) - sum(COALESCE(b_seconds, 0)) as total
		, sum(case
			 	when COALESCE(b_level, '') != '' and ranking = 1 and a_end > b_end then 2
			 	else 1
			 end) as count
	from cte_overlap_sleep_level
	group by logId, a_level, a_start, a_end
),
cte_aggregated_at_sleep_level as (
	select
		logId
		, a_level as sleep_level
		, round(sum(total) / 60) as minutes
		, sum(count) as count
	from cte_aggregated_at_sleep_level_interval
	group by logId, a_level
)
select
	*
from cte_sleep_overview
join cte_aggregated_at_sleep_level using(logId)