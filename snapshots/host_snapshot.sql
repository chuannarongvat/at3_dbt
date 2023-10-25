{% snapshot host_snapshot %}

{{
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key = 'listing_id',
        updated_at='scraped_date'
    )
}}

select scraped_date, listing_id, host_id, host_name, host_since, host_is_superhost, host_neighbourhood 
from {{ source('raw', 'listings') }}

{% endsnapshot %}