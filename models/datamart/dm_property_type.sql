with base as (
    select
        month_year,
        property_type,
        room_type,
        accommodates,
        review_scores_rating,
        host_id,
        host_is_superhost,
        case when has_availability = 't' then 1 else 0 end as active_listing,
        price,
        case when has_availability = 't' then 30 - availability_30 else 0 end as number_of_stays,
        case when has_availability = 't' then (30 - availability_30) * price else 0 end as estimated_revenue_per_listing
    from {{ ref('fact_listing') }}  
),

aggregation as (
    select
        property_type,
        room_type,
        accommodates,
        month_year,
        count(*) as total_listings,
        sum(active_listing) as total_active_listings,
        round(avg(case when active_listing = 1 and price > 0 then price else null end)::numeric, 2)as avg_price_for_active_listings,
        min(case when active_listing = 1 and price > 0 then price else null end) as min_price_for_active_listings,
        max(case when active_listing = 1 and price > 0 then price else null end) as max_price_for_active_listings,
        percentile_cont(0.5) within group (order by case when active_listing = 1 then price else null end) as median_price_for_active_listings,
        count(distinct case when host_is_superhost = 't' then host_id else null end) as superhost_listings,
        count(distinct host_id) as total_distinct_hosts,
        coalesce(round(avg(case when active_listing = 1 and review_scores_rating > 0 then review_scores_rating end)::numeric, 2), -1) as avg_review_score_for_active_listings,
        sum(number_of_stays) as total_number_of_stays,
        sum(estimated_revenue_per_listing) as avg_estimated_revenue_for_active_listings
    from base
    group by property_type, room_type, accommodates, month_year
),

prev_month as (
    select
        *,
        total_listings - total_active_listings as total_inactive_listings,
        lag(total_active_listings) over (partition by property_type, room_type, accommodates order by month_year) as prev_month_active_listings,
        lag(total_listings - total_active_listings) over (partition by property_type, room_type, accommodates order by month_year) as prev_month_inactive_listings
    from
        aggregation
)

select
    property_type,
    room_type,
    accommodates,
    month_year,
    round(((total_active_listings::float / total_listings::float) * 100)::numeric, 2) as active_listings_rate,
    min_price_for_active_listings,
    max_price_for_active_listings,
    median_price_for_active_listings,
    avg_price_for_active_listings,
    total_distinct_hosts,
    round(((superhost_listings::float / total_distinct_hosts::float) * 100)::numeric, 2) as superhost_rate,
    avg_review_score_for_active_listings,
    coalesce(
        case when prev_month_active_listings = 0 then null 
        else round(((((total_active_listings - prev_month_active_listings)::float) / prev_month_active_listings::float) * 100)::numeric, 2) 
        end, 0
    ) as pct_change_active_listings,
    coalesce(
        case when prev_month_inactive_listings = 0 then null 
        else round(((((total_inactive_listings - prev_month_inactive_listings)::float) / prev_month_inactive_listings::float) * 100)::numeric, 2) 
        end, 0
    ) as pct_change_inactive_listings,
    total_number_of_stays,
    avg_estimated_revenue_for_active_listings
from prev_month
order by property_type, room_type, accommodates, month_year