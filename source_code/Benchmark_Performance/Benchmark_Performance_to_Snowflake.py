###################################################################
# load_benchmark_to_snowflake.py



import argparse
import datetime as dt
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
from dotenv import load_dotenv
import os

## pls define ur config env path in here, and put the snowflake login info inside
ENV_PATH = Path(r"C:\Boston University_資料夾\BA888_Capstone\Target_Date_Fund\source_code\Benchmark_Performance\Rick_config.env") # change to ur path
assert ENV_PATH.exists(), f".env not exist：{ENV_PATH}"
load_dotenv(ENV_PATH)


# Import the fetcher (no files written)
from .Benchmark_Performance_table import get_benchmark_performance


# Load Snowflake credentials
# load_dotenv("Rick_config.env")

def get_snowflake_connection():
    """
    Create a Snowflake connection using environment variables.
    """
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

def get_last_date_for_code(cs, code: str):
    """
    Query Snowflake for the last (max) HISTORYDATE1 for a given BENCHMARKCODE.
    Returns a date object or None.
    """
    cs.execute(
        """
        SELECT MAX(HISTORYDATE1)
        FROM AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE
        WHERE BENCHMARKCODE = %s
        """,
        (code,)
    )
    last = cs.fetchone()[0]
    if last is None:
        return None
    if isinstance(last, dt.datetime):
        return last.date()
    return last  # already a date

def orchestrate_benchmark_load(
    tickers: list[str],
    full_start_date: str,
    end_date: str,
    frequency: str = "D"
):
    """
    For each ticker:
      - Determine the start date as (last_date_in_snowflake + 1) or full_start_date if no data.
      - Fetch data in-memory via get_benchmark_performance() without writing to disk.
      - Concatenate all new rows and load them into Snowflake using a temp table + MERGE.
    """
    ctx = get_snowflake_connection()
    cs = ctx.cursor()
    try:
        all_dfs = []
        today = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

        for ticker in tickers:
            code = ticker.lstrip("^")
            last = get_last_date_for_code(cs, code)
            if last:
                start = last + dt.timedelta(days=1)
                print(f"► {code}: last date in Snowflake = {last}, fetch start = {start}")
            else:
                start = dt.datetime.strptime(full_start_date, "%Y-%m-%d").date()
                print(f"► {code}: no existing data, fetch start = {start}")

            if start > today:
                print(f"► {code}: no new data (start {start} > end {today})")
                continue

            start_str = start.strftime("%Y-%m-%d")
            print(f"► Fetching {code} from {start_str} to {end_date} (freq={frequency})")
            df = get_benchmark_performance(ticker, start_str, end_date, frequency)

            if not df.empty:
                all_dfs.append(df)
            else:
                print(f"► {code}: fetched 0 rows")

        if not all_dfs:
            print("⚠️ No new data for any ticker.")
            return

        df_all = pd.concat(all_dfs, ignore_index=True)

        # Create temporary table that mirrors target schema
        print("► Creating temporary table tmp_benchmarkperformance")
        cs.execute("""
            CREATE OR REPLACE TEMPORARY TABLE tmp_benchmarkperformance
            LIKE AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE
        """)

        # Bulk insert into temp table
        cols = df_all.columns.tolist()
        placeholder = ", ".join(["%s"] * len(cols))
        insert_sql = f"""
            INSERT INTO tmp_benchmarkperformance ({', '.join(cols)})
            VALUES ({placeholder})
        """
        data = [tuple(row) for row in df_all.itertuples(index=False, name=None)]
        print(f"► Inserting {len(data)} rows into tmp_benchmarkperformance")
        cs.executemany(insert_sql, data)
        ctx.commit()

        # MERGE into target to avoid duplicates defensively
        print("► Merging into AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE")
        merge_sql = f"""
        MERGE INTO AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE AS tgt
        USING tmp_benchmarkperformance AS src
          ON tgt.BENCHMARKCODE = src.BENCHMARKCODE
         AND tgt.HISTORYDATE1  = src.HISTORYDATE1
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(cols)})
          VALUES ({', '.join('src.' + c for c in cols)})
        """
        cs.execute(merge_sql)
        print(f"✔ Merge inserted {cs.rowcount} new rows")

    finally:
        cs.close()
        ctx.close()

def main():
    parser = argparse.ArgumentParser(description="Fetch benchmarks in-memory and load into Snowflake.")
    parser.add_argument("--tickers", nargs="+", default=["^GSPC", "AGG"], help="List of tickers")
    parser.add_argument("--full-start", default="2004-01-01", help="Full start date (used if code has no data)")
    parser.add_argument("--end", default=dt.date.today().strftime("%Y-%m-%d"), help="End date YYYY-MM-DD")
    parser.add_argument("--freq", default="D", help="Frequency (D, W, M)")
    args = parser.parse_args()

    orchestrate_benchmark_load(
        tickers=args.tickers,
        full_start_date=args.full_start,
        end_date=args.end,
        frequency=args.freq
    )

if __name__ == "__main__":
    main()


#######################################
