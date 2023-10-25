{{
    config(
        unique_key='(host_id, listing_id)'
    )
}}

select * from {{ ref('host_stg') }}