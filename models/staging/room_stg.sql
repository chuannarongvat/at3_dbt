{{
    config(
        unique_key='listing_id'
    )
}}

with source as (
    select
        listing_id,
        room_type,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('room_snapshot') }}
    where listing_id is not null
)

select
    room_type,
    dbt_valid_from,
    dbt_valid_to
from source