with minutes_converted as (
	select
		recorded_time
		, level_light / 60 as "light"
		, level_rem / 60 as "rem"
		, level_deep / 60 as "deep"
		, level_wake / 60 as "wake"
	from {{ ref('int_sleep') }}
	{% if is_incremental() %}

		where recorded_time > (select COALESCE(max(recorded_time), '2023-02-21') from {{ this }})

	{% endif %}
)
select
	*
from minutes_converted