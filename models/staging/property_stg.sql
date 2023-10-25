{{
    config(
        unique_key='listing_id'
    )
}}

with source as (
    select
        listing_id,
        property_type,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('property_snapshot') }}
    where listing_id is not null
)

select
    property_type,
    dbt_valid_from,
    dbt_valid_to
from source