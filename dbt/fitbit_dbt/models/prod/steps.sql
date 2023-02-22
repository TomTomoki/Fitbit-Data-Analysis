with org_stg_steps as (
	select
		*
    from {{ ref('stg_steps') }}
	{% if is_incremental() %}

		where recorded_time > (select COALESCE(max(recorded_time), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	recorded_time
	, num_steps
from org_stg_steps