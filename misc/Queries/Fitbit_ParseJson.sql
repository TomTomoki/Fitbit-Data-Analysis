-- Sleep
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
	from landing.sleep
),
cte_union_shortData_data as (
	select
		load_json #> '{sleep,0}' ->> 'logId' as logId
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'dateTime' as dateTime
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'level' as level
		, cast(json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{shortData}') ->> 'seconds' as integer) as seconds
	from landing.sleep
	union
	select
		load_json #> '{sleep,0}' ->> 'logId' as logId
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'dateTime' as dateTime
		, json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'level' as level
		, cast(json_array_elements(load_json #> '{sleep,0}' -> 'levels' #> '{data}') ->> 'seconds' as integer) as seconds
	from landing.sleep
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
join cte_aggregated_at_sleep_level using(logId);



-- Steps
with expanded as (
	select
		load_json -> 'activities-steps' -> 0 ->> 'dateTime' as recorded_date
		, load_json -> 'activities-steps' -> 0 ->> 'value' as total_steps
		, json_array_elements(load_json -> 'activities-steps-intraday' -> 'dataset') as steps_detail
	from landing.steps
)
, json_parsed as (
	select
		recorded_date
		, total_steps
		, cast(steps_detail ->> 'time' as time) as recorded_time
		, cast(steps_detail ->> 'value' as integer) as steps
	from expanded
)
, min30_interval_converted as (
	select
		recorded_date::date + recorded_time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time)) + 1800) / 1800) * 1800) as min30_interval
		, steps
	from json_parsed
)
select
min30_interval as recorded_time
, sum(steps)
from min30_interval_converted
group by min30_interval
order by min30_interval;
-- ### For houly aggregation
-- select
-- 	recorded_date::date + date_trunc('hour', recorded_time) as recorded_time
-- 	, sum(steps) as steps
-- from json_parsed
-- group by recorded_date::date + date_trunc('hour', recorded_time)
-- order by recorded_time;



-- Calories
with expanded as (
	select
		load_json -> 'activities-calories' -> 0 ->> 'dateTime' as recorded_date
		, load_json -> 'activities-calories' -> 0 ->> 'value' as total_calories
		, json_array_elements(load_json -> 'activities-calories-intraday' -> 'dataset') as calories_detail
	from landing.calories
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
		recorded_date::date + recorded_time as original_datetime
		, to_timestamp(floor((extract(epoch from (recorded_date::date + recorded_time)) + 1800) / 1800) * 1800) as min30_interval
		, mets
		, calories
	from json_parsed
)
select
	min30_interval as recorded_time
	, round(avg(mets), 2) as avg_mets
	, round(sum(calories), 2) as sum_calories
from min30_interval_converted
group by min30_interval
order by min30_interval;