with org_stg_heartrate as (
	select
		*
    from {{ ref('stg_heartrate') }}
	{% if is_incremental() %}

		where recorded_time > (select COALESCE(max(recorded_time), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	recorded_time
	, avg_heartrate
	, max_heartrate
	, min_heartrate
from org_stg_heartrate