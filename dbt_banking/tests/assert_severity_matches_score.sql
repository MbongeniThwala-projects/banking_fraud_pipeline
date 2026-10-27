select transaction_id, fraud_risk_score, alert_severity
from {{ ref('fct_fraud_alerts') }}
where (alert_severity = 'CRITICAL' and fraud_risk_score < 75)
    or (alert_severity = 'HIGH' and (fraud_risk_score < 45 or fraud_risk_score >= 75))
    or (alert_severity = 'MEDIUM' and fraud_risk_score >= 45)