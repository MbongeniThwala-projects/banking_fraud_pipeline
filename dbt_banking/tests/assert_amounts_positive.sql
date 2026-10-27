select transaction_id, transaction_amount
from {{ ref('stg_bank_transactions') }}
where transaction_amount <= 0