with org_stg_sedentary as (
	select
		*
    from {{ ref('stg_sedentary') }}
	{% if is_incremental() %}

		where recorded_time > (select COALESCE(max(recorded_time), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	recorded_time
	, sedentary as minutes_sedentary
from org_stg_sedentary