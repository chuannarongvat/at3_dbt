{% snapshot property_snapshot %}

{{
    config(
        target_schema='raw',
        strategy='timestamp',
        unique_key='listing_id',
        updated_at='scraped_date'
    )
}}

select scraped_date, listing_id, property_type
from {{ source('raw', 'listings') }}

{% endsnapshot %}
