with transactions as (
    select
        transaction_id,
        account_id,
        customer_id,
        transaction_timestamp,
        transaction_amount,
        transaction_type,
        merchant_category_code,
        device_ip,
        ip_country_code,
        ip_region,
        card_present_flag,
        ingested_at
    from {{ ref('stg_bank_transactions') }}
),

--pairs each transaction with every transaction on the same account
-- in the preceding hour (including itself), then aggregates per transaction
hourly_window as (
    select
    cur.transaction_id,
        count(*) as transaction_count_past_1h,
        sum(win.transaction_amount) as total_spend_past_1h,
        avg(win.transaction_amount) as avg_spend_past_1h,
        count(distinct win.ip_country_code) as distinct_countries_past_1h
    from transactions as cur
    inner join transactions as win
        on cur.account_id = win.account_id
        and win.transaction_timestamp between
            timestamp_sub(cur.transaction_timestamp, interval 1 hour)
            and cur.transaction_timestamp
    group by cur.transaction_id
),

previous_transaction as (
    select 
        transaction_id,
        lag(transaction_timestamp) over (
            partition by account_id order by transaction_timestamp
        ) as prev_transaction_timestamp,
        lag(ip_country_code) over (
            partition by account_id order by transaction_timestamp
        ) as prev_ip_country_code
    from transactions
),

combined as (
    select
        t.*,
        h.transaction_count_past_1h,
        h.total_spend_past_1h,
        h.avg_spend_past_1h,
        h.distinct_countries_past_1h,
        p.prev_transaction_timestamp,
        p.prev_ip_country_code,
        timestamp_diff(
            t.transaction_timestamp, p.prev_transaction_timestamp, minute
        ) as mins_since_last_transaction

    from transactions as t
    inner join hourly_window as h on t.transaction_id = h.transaction_id
    inner join previous_transaction as p on t.transaction_id = p.transaction_id
),

flagged as (
    select
        *,
        transaction_count_past_1h >= 5 as is_high_frequency_velocity,
        total_spend_past_1h >= 10000 as is_high_value_velocity,
        (
            prev_ip_country_code is not null
            and ip_country_code != prev_ip_country_code
            and mins_since_last_transaction < 15            
        ) as is_impossible_travel
    from combined
),

scored as (
    select 
        *,
        least(100, (
            case when is_high_frequency_velocity then 30 else 0 end
            + case when is_high_value_velocity then 25 else 0 end
            + case when distinct_countries_past_1h > 1 then 25 else 0 end
            + case when is_impossible_travel then 25 else 0 end
            + case when card_present_flag = false and transaction_amount > 2500 then 20 else 0 end
        )) as fraud_risk_score
    from flagged
)

select * from scored