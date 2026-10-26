with source as (
    select * from {{ source('raw_banking', 'transactions_landing') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_id
            order by ingested_at desc
        ) as row_num
    from source
),

cleaned as (
    select
        transaction_id,
        account_id,
        customer_id,
        transaction_timestamp,
        cast(round(transaction_amount, 2) as numeric) as transaction_amount,
        transaction_type,
        merchant_category_code,
        device_ip,
        ip_country_code,
        case ip_country_code
            when 'ZA' then 'Southern Africa'
            when 'US' then 'Norht America'
            when 'RU' then 'Eastern Europe'
            else "Other"
        end as ip_regioin,
        card_present_flag,
        ingested_at
    from deduplicated
    where row_num = 1
)

select * from cleaned