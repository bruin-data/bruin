"""@bruin

name: frankfurter.print_results

depends:
    - frankfurter.latest_rates
    - frankfurter.currencies

@bruin"""

import duckdb
import pandas as pd

def materialize():

    db_path = "duckdb.db"

    conn = duckdb.connect(db_path)

    query_1 = (
        "SELECT currency_code as 'CURRENCY CODE',"
        "currency_name as 'CURRENCY NAME'"
        "FROM frankfurter.currencies;"
    )

    df = conn.execute(query_1).fetchdf()

    print("\n")
    print("AVAILABLE CURRENCIES:")
    print(df.to_string(index=False))

    query_2 = (
        "SELECT date AS 'DATE', "
        "currency_name AS 'CURRENCY CODE', "
        "rate AS 'RATE' "
        "FROM frankfurter.latest_rates "
        "WHERE currency_name IN ('EUR', 'GBP', 'IDR', 'USD');"
    )

    df = conn.execute(query_2).fetchdf()

    print("\n")
    print("PRINTING LATEST RATES FOR SELECTED CURRENCIES:")
    print(df.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    materialize()