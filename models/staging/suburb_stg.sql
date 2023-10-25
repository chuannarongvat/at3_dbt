{{
    config(
        unique_key='(lga_name, suburb_name)'
    )
}}

with source as (
    select
        initcap(lga_name) as lga_name,
        initcap(suburb_name) as suburb_name
    from {{ source('raw', 'nsw_lga_suburb') }}
),

duplicates_suburb_and_lga as (
    select
        lga_name,
        suburb_name
    from source
    group by lga_name, suburb_name
    having count(*) > 1
),

duplicates_suburb as (
    select suburb_name
    from source
    group by suburb_name
    having count(suburb_name) > 1
),

staged_data as (
    select * from source
    where (lga_name, suburb_name) not in (select lga_name, suburb_name from duplicates_suburb_and_lga)
    and suburb_name not in (select suburb_name from duplicates_suburb)
    and lga_name is not null
    and suburb_name is not null
)

select * from staged_data