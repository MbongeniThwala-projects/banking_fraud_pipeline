select transaction_id, fraud_risk_score
from {{ ref('fct_fraud_alerts') }}
where fraud_risk_score < 20