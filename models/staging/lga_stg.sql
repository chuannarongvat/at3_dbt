{{
    config(
        unique_key='lga_code'
    )
}}

with source as (
    select * from {{ source('raw', 'nsw_lga_code') }}
),

duplicates_code as (
    select
        lga_code
    from source
    group by lga_code
    having count(lga_code) > 1
),

duplicates_name as (
    select
        lga_name
    from source
    group by lga_name
    having count(lga_name) > 1
),

staged_data as (
    select * from source
    where lga_code not in (select lga_code from duplicates_code)
    and lga_name not in (select lga_name from duplicates_name)
    and lga_code is not null
    and lga_name is not null
)

select * from staged_data