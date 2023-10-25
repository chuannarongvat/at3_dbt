{{
    config(
        unique_key='lga_code'
    )
}}

select
    l.lga_code,
    l.lga_name,
    s.suburb_name
from
    {{ ref('lga_stg') }} l
inner join
    {{ ref('suburb_stg') }} s on l.lga_name = s.lga_name