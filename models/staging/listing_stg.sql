{{ 
    config( 
        unique_key='listing_id' 
    ) 
}}

with source as (
    select
        listing_id,
        to_char(to_date(scraped_date, 'YYYY-MM-DD'), 'YYYY-MM') as month_year,
        host_id,
        initcap(listing_neighbourhood) as listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        case when review_scores_rating = 'NaN' then -1 else review_scores_rating end as review_scores_rating,
        case when review_scores_accuracy = 'NaN' then -1 else review_scores_accuracy end as review_scores_accuracy,
        case when review_scores_cleanliness = 'NaN' then -1 else review_scores_cleanliness end as review_scores_cleanliness,
        case when review_scores_checkin = 'NaN' then -1 else review_scores_checkin end as review_scores_checkin,
        case when review_scores_communication = 'NaN' then -1 else review_scores_communication end as review_scores_communication,
        case when review_scores_value = 'NaN' then -1 else review_scores_value end as review_scores_value
    from {{ source('raw', 'listings') }}
    where
        listing_id is not null
)

select * from source

-- price = 0: 12
-- number_of_reviews = 0 : 118,269
-- reveiws_scores_rating = 'NaN': 132,820
-- reveiws_score_accuracy = 'NaN' : 133,470
-- reveiws_score_cleanliness = 'NaN' : 133,280
-- reveiws_score_checkin = 'NaN' : 133,600
-- reveiws_score_communication = 'NaN' : 133,277
-- reveiws_score_value = 'NaN' : 133,685

