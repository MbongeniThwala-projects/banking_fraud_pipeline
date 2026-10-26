with source as (
    select * from {{ source('raw_banking', 'transactions_landing') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_id
        )
)