with scored as (
    select * from {{ ref('int_account_rolling_velocity') }}
),

alerts as (
    select 
        transaction_id,
        account_id,
        customer_id,
        transaction_timestamp,
        transaction_amount,
        transaction_type,
        merchant_category_code,
        ip_country_code,
        ip_regioin,
        card_present_flag,
        transaction_count_past_1h,
        total_spend_past_1h,
        distinct_countries_past_1h,
        is_high_frequency_velocity,
        is_high_value_velocity,
        is_impossible_travel,
        fraud_risk_score,

        case
            when fraud_risk_score >= 75 then 'CRITICAL'
            when fraud_risk_score >= 45 then 'HIGH'
            else 'MEDIUM'
        end as alert_severity,
        rtrim(concat(
            if(is_high_frequency_velocity, 'HIGH_FREQUENCY; ', ''),
            if(is_high_value_velocity, 'HIGH_VALUE_VELOCITY; ', ''),
            if(is_impossible_travel, 'IMPOSSIBLE_TRAVEL; ', ''),
            if(distinct_countries_past_1h > 1, 'MULTI_COUNTRY; ', ''),
            if(card_present_flag = false and transaction_amount > 2500, 'LARGE_CARD_NOT_PRESENT; ', '')
        ), '; ') as alert_reasons,
        current_timestamp() as alert_generated_at
    from scored
    where fraud_risk_score >= 20
)
select * from alerts