{{ 
    config(
        unique_key='listing_id'
    ) 
}}

with source as (
    select * 
    from {{ ref('host_snapshot') }}
    where listing_id is not null
),

ranked_hosts as (
    select *,
        row_number() over (partition by host_id order by listing_id) as rn
    from source
)

select
    host_id,
    case when host_name = 'NaN' then 'Unknown' else host_name end as host_name,
    case 
        when host_since = 'NaN' then '0'
        when host_since ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' then to_char(to_date(host_since, 'dd/mm/yyyy'), 'yyyy-mm-dd') 
        else null 
    end as host_since,
    case when host_is_superhost = 'NaN' then 'Unknown' else host_is_superhost end as host_is_superhost,
    case 
        when host_neighbourhood = 'NaN' then 'Unknown' 
        else initcap(host_neighbourhood) 
    end as host_neighbourhood,
    dbt_valid_from,
    dbt_valid_to
from ranked_hosts
where rn = 1
