{{
    config(
        unique_key='listing_id'
    )
}}

select
    f.listing_id,
    f.month_year,
    h.host_id,
    h.host_is_superhost,
    h.host_neighbourhood,
    case
        when h.host_neighbourhood = 'Unknown' then 'Unknown'
        else s.lga_name
    end as host_neighbourhood_lga,
    f.listing_neighbourhood,
    n.lga_code as listing_lga_code,
    f.property_type,
    f.room_type,
    f.accommodates,
    f.price,
    f.has_availability,
    f.availability_30,
    f.number_of_reviews,
    f.review_scores_rating,
    f.review_scores_accuracy,
    f.review_scores_cleanliness,
    f.review_scores_checkin,
    f.review_scores_communication,
    f.review_scores_value
from {{ ref('listing_stg') }} f
left join {{ ref('dim_host') }} h
on f.host_id = h.host_id
left join (
    select lga_name, min(lga_code) as lga_code
    from {{ ref('dim_nsw') }}
    group by lga_name
) n
on f.listing_neighbourhood = n.lga_name
left join {{ ref('dim_nsw')}} s
on h.host_neighbourhood = s.suburb_name
