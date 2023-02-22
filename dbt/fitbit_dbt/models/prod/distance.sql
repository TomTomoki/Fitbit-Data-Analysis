with org_stg_distance as (
	select
		*
    from {{ ref('stg_distance') }}
	{% if is_incremental() %}

		where recorded_time > (select COALESCE(max(recorded_time), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	recorded_time
	, distance_meters as distance_in_meters
from org_stg_distance