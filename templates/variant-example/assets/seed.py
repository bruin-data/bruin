"""@bruin
name: "{{ var.client }}_seed_{{ var.region }}"
type: python
@bruin"""

from datetime import datetime, timedelta

import duckdb

conn = duckdb.connect("/tmp/variants_demo.duckdb")

now = datetime.now()
data = {
    "us": [
        (1, "alice@alpha.com",  now - timedelta(days=1),  "alpha"),
        (2, "bob@alpha.com",    now - timedelta(days=3),  "alpha"),
        (3, "carol@alpha.com",  now - timedelta(days=45), "alpha"),
        (4, "dave@other.com",   now - timedelta(days=2),  "other"),
    ],
    "eu": [
        (1, "anna@beta.com",    now - timedelta(days=1),  "beta"),
        (2, "ben@beta.com",     now - timedelta(days=3),  "beta"),
        (3, "claire@beta.com",  now - timedelta(days=45), "beta"),
        (4, "dora@other.com",   now - timedelta(days=2),  "other"),
    ],
    "ap": [
        (1, "akira@gamma.com",  now - timedelta(days=1),  "gamma"),
        (2, "bo@gamma.com",     now - timedelta(days=3),  "gamma"),
        (3, "chen@gamma.com",   now - timedelta(days=45), "gamma"),
        (4, "deepak@other.com", now - timedelta(days=2),  "other"),
    ],
}

for region, rows in data.items():
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS analytics_{region}")
    conn.execute(f"DROP TABLE IF EXISTS analytics_{region}.raw_users")
    conn.execute(f"""
        CREATE TABLE analytics_{region}.raw_users (
            user_id      INTEGER,
            email        VARCHAR,
            signed_up_at TIMESTAMP,
            tenant       VARCHAR
        )
    """)
    conn.executemany(
        f"INSERT INTO analytics_{region}.raw_users VALUES (?, ?, ?, ?)", rows
    )

conn.close()
print("Seeded all regions (us, eu, ap)")
