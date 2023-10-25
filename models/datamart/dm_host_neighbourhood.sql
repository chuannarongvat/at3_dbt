with base as (
    select
        month_year,
        host_neighbourhood_lga,
        host_id,
        price,
        (30 - availability_30) * price as estimated_revenue
    from {{ ref('fact_listing') }}  
),

aggregation as (
    select
        month_year,
        host_neighbourhood_lga,
        count(distinct host_id) as total_distinct_hosts,
        sum(estimated_revenue) as estimated_revenue
    from base
    group by host_neighbourhood_lga, month_year
)

select
    host_neighbourhood_lga,
    month_year,
    total_distinct_hosts,
    estimated_revenue,
    round((estimated_revenue / total_distinct_hosts)::numeric, 2) as estimated_revenue_per_host
from aggregation
order by host_neighbourhood_lga, month_year

