
# This code aim to construct the orchestration for benchmarkperformance table
# The flaw chart can see the Benchmark_Orchestration.puml
## Pls execute below code together(need to first setup the log in info in ur local env.)

import datetime as dt
import pandas as pd
import yfinance as yf
import snowflake.connector
from dotenv import load_dotenv
import os
# laod .env setup 
load_dotenv("Rick_config.env")

def get_snowflake_connection():
     return snowflake.connector.connect(
         user=os.getenv("SNOWFLAKE_USER"),
         password=os.getenv("SNOWFLAKE_PASSWORD"),
         account=os.getenv("SNOWFLAKE_ACCOUNT"),
         warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
         database=os.getenv("SNOWFLAKE_DATABASE"),
         schema=os.getenv("SNOWFLAKE_SCHEMA"),
         role=os.getenv("SNOWFLAKE_ROLE")
     )


def get_benchmark_performance(
    benchmark_ticker: str,
    start_date: str,
    end_date: str,
    frequency: str = "D"
) -> pd.DataFrame:
    raw = yf.download(
        benchmark_ticker,
        start=start_date,
        end=end_date,
        interval="1d",
        progress=False,
        auto_adjust=True
    )
    price = raw["Close"]
    if frequency != "D":
        price = price.resample(frequency).last()
    # reshape column structure and data type to match the structure in snowflake
    df = price.reset_index()
    df.columns = ["HISTORYDATE1", "VALUE"]

    ## split to date and time
    ts = pd.to_datetime(df["HISTORYDATE1"])  
    ## Save Date to HISTORYDATE1 to make sure the structure match in snowflake
    df["HISTORYDATE1"] = ts.dt.strftime("%Y-%m-%d")
    ## Save Time to HISTORYDATE and chage type to TIMESTAMP_NTZ to make sure the structure match in snowflake
    df["HISTORYDATE"]  = ts.dt.strftime("%Y-%m-%d %H:%M:%S")


    df["BENCHMARKCODE"]        = benchmark_ticker.lstrip("^")
    df["PERFORMANCEDATATYPE"]  = "Prices"
    df["CURRENCYCODE"]         = "USD"
    df["CURRENCY"]             = "US Dollar"
    df["PERFORMANCEFREQUENCY"] = frequency

    cols = [
        "BENCHMARKCODE","PERFORMANCEDATATYPE",
        "CURRENCYCODE","CURRENCY",
        "PERFORMANCEFREQUENCY",
        "VALUE",
        "HISTORYDATE1","HISTORYDATE"
    ]
    return df[cols]

# Setting up the Orchestration logistic 
def orchestrate_benchmark_load(
    tickers: list[str],
    full_start_date: str,
    end_date: str,
    frequency: str = "D"
):
    ctx = get_snowflake_connection()
    cs = ctx.cursor()
    try:
        all_dfs = []
        today = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

        for ticker in tickers:
            code = ticker.lstrip("^")
            # Check Whether specific data of the ticker exists or not
            cs.execute(
                "SELECT MAX(HISTORYDATE1) FROM AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE "
                "WHERE BENCHMARKCODE = %s",
                (code,)
            )
            last = cs.fetchone()[0]
            if last:
            # If its is datetime.datetime, then get .date()
                last_date = last.date() if isinstance(last, dt.datetime) else last
                start = last_date + dt.timedelta(days=1)
            else:
                start = dt.datetime.strptime(full_start_date, "%Y-%m-%d").date()

            if start > today:
                print(f"► {code}: no new data")
                continue
            ## If exists, just insert the missing data
            start_str = start.strftime("%Y-%m-%d")
            print(f"► Fetching {code} from {start_str} to {end_date}")
            df = get_benchmark_performance(ticker, start_str, end_date, frequency)
            if not df.empty:
                all_dfs.append(df)
        # If not exists, insert all dataframe
        if not all_dfs:
            print("⚠️ No new data for any ticker.")
            return

        # concat it
        df_all = pd.concat(all_dfs, ignore_index=True)

    

        # create tmp in snowflake
        print("► Creating temp table tmp_benchmarkperformance")
        cs.execute("""
            CREATE OR REPLACE TEMPORARY TABLE tmp_benchmarkperformance
            LIKE AST_MULTIASSET_DB.DBO.BENCHMARKPERFORMANCE
        """)

        # Batch Insert
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

        # MERGE to skip the duplicate data
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
        print(f"► Merge inserted {cs.rowcount} new rows")

    finally:
        cs.close()
        ctx.close()
if __name__ == "__main__":
    TICKERS = ["^GSPC", "AGG"]
    FULL_START_DATE = "2004-01-01"
    END_DATE = dt.date.today().strftime("%Y-%m-%d")
    orchestrate_benchmark_load(TICKERS, FULL_START_DATE, END_DATE, "D")
