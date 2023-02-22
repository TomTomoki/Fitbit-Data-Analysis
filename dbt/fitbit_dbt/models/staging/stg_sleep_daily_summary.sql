with sleep_overview as (
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
		, load_json -> 'summary' ->> 'totalMinutesAsleep' as totalMinutesAsleep
		, load_json -> 'summary' ->> 'totalSleepRecords' as totalSleepRecords
		, load_json -> 'summary' ->> 'totalTimeInBed' as totalTimeInBed
		, load_timestamp
	from landing.sleep
	{% if is_incremental() %}

		where cast(load_json #> '{sleep,0}' ->> 'dateOfSleep' as date) > (select COALESCE(max(dateOfSleep), '2022-09-30') from {{ this }})

	{% endif %}
)
select
    cast(dateOfSleep as DATE)
	, duration
	, efficiency
	, endTime
	, isMainSleep
	, logId
	, minutesAfterWakeup
	, minutesAsleep
	, minutesAwake
	, minutesToFallAsleep
	, startTime
	, timeInBed
	, totalMinutesAsleep
	, totalSleepRecords
	, totalTimeInBed
	, load_timestamp
from sleep_overview