from fastapi import FastAPI, Query
# from typing import Optional # we will use this instead on str for date, to prevent a str input as date
from faker import Faker
import random
import uuid
from datetime import datetime, timezone, timedelta

app = FastAPI(title="Core Banking Transaction API")
fake = Faker()

TRANSACTION_TYPE = ["CARD_PRESENT", "CARD_NOT_PRESENT", "EFT_TRANSFER", "ATM_WITHDRAWAL"]
MERCHANT_CATEGORIES = ["GROCERY", "ELECTRONICS", "LUXURY_GOODS", "GAS_STATION", "ONLINE_RETAIL", "GAMBLING"]
RISKY_MERCHANTS = ["GAMBLING", "ELECTRONICS", "LUXURY_GOODS"]
HOME_COUNTRY = "ZA"
FOREIGN_COUNTRIES = ["RU", "CN", "NG", "GB"]

ACCOUNTS = [(f"ACC-{10000 + i}", f"CUST-{1000 + i}") for i in range(50)]

def build_record(account_id, customer_id, ts, amount, merchant, country, card_present):
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_id": customer_id,
        "transaction_timestamp": ts.isoformat(),
        "transaction_amount": amount,
        "transaction_type": random.choice(TRANSACTION_TYPE),
        "merchant_category_code": merchant,
        "device_ip": fake.ipv4_public(),
        "ip_country_code":country,
        "card_present_flag": card_present
    }

def make_normal(base_time):
    account_id, customer_id = random.choice(ACCOUNTS)
    ts = base_time - timedelta(minutes=random.randint(0, 1440))
    if random.random() < .02:
        #isolated high-value anomaly
        return build_record(
            account_id, customer_id, ts,
            round(random.uniform(5000.0, 25000.0), 2),
            "GAMBLING", random.choice(FOREIGN_COUNTRIES), False,
        )
    return build_record(
        account_id, customer_id, ts,
        round(random.uniform(1.50, 850.0), 2),
        random.choice(MERCHANT_CATEGORIES), HOME_COUNTRY, random.choice([True, False])
    )

def make_burst(base_time):
    '''One home-county transactioin, then a rapid run from a foreign country.'''
    account_id, customer_id = random.choice(ACCOUNTS)
    foreign = random.choice(FOREIGN_COUNTRIES)
    ts = base_time - timedelta(minutes=random.randint(90, 1380))
    rows = []
    for i in range(random.randint(5, 7)):
        if i == 0:
            rows.append(build_record(
                account_id, customer_id, ts,
                round(random.uniform(1.50, 850.0), 2),
                random.choice(MERCHANT_CATEGORIES), HOME_COUNTRY, True,
            ))
        else:
            rows.append(build_record(
                account_id, customer_id, ts,
                round(random.uniform(1500.0, 4500.0), 2),
                random.choice(RISKY_MERCHANTS), foreign, False,
            ))
        ts += timedelta(minutes=random.randint(1, 8))
    return rows

# @app.get("/v1/transactions")
# # def get_transactions(limit: int = Query(default=100, ge=1, le=1000), date: str = Query(default....)):
@app.get("/v1/transactions")
def get_transactions(limit: int = Query(default=100, ge=1, le=1000), date: str = Query(default=None, description="Format: YYYY-MM-DD")):
    '''Simulate real time core banking transaction logs with occasional fraud signatures'''
    records = []
    base_time = datetime.now(timezone.utc)

    if date:
        try:
            parsed_date = datetime.striptime(date, "%Y-%m-%d")
            base_time = base_time.replace(year=parsed_date.year, month=parsed_date.month, day=parsed_date.day)
        except ValueError:
            pass

    if limit >= 20 and random.random() < .8:
        records.extend(make_burst(base_time))

    while len(records) < limit:
        records.append(make_normal(base_time))

    random.shuffle(records)

    return {"status": "success", "count": len(records), "data": records}

    # for _ in range(limit):
    #     #lets inject 5% high-value fraud anomaly chance
    #     is_anomaly = random.random() < .05
    #     amount = round(random.uniform(5000.0, 25000.0), 2) if is_anomaly else round(random.uniform(1.50, 850.0), 2)

    #     random_offset = timedelta(minutes=random.randint(0, 1440))
    #     transaction_timestamp = (base_time - random_offset).isoformat()

    #     records.append({
    #         "transaction_id": str(uuid.uuid4()),
    #         "account_id": f"ACC-{random.randint(1000, 99999)}",
    #         "customer_id": f"CUST-{random.randint(1000, 9999)}",
    #         "transaction_timestamp": transaction_timestamp,
    #         "transaction_amount": amount,
    #         "transaction_type": random.choice(TRANSACTION_TYPE),
    #         "merchant_category_code": "GAMBLING" if is_anomaly else random.choice(MARCHANT_CATEGORIES),
    #         "device_ip": fake.ipv4_public(),
    #         "ip_country_code": fake.random_element(["RU", "CN", "NG", "GB"]) if is_anomaly else "ZA",
    #         "card_present_flag": False if is_anomaly else random.choice([True, False])
    #     })

    # return {
    #     "status": "success",
    #     "count": len(records),
    #     "data": records
    # }

@app.get("/health")
def health_check():
    return {"status": "healthy"}