with org_stg_calories as (
	select
		*
    from {{ ref('stg_calories') }}
	{% if is_incremental() %}

		where recorded_time > (select COALESCE(max(recorded_time), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	recorded_time
	, avg_mets
	, sum_calories as total_calories
from org_stg_calories