# benchmark_fetcher.py
import pandas as pd
import yfinance as yf

def get_benchmark_performance(
    benchmark_ticker: str,
    start_date: str,
    end_date: str,
    frequency: str = "D"
) -> pd.DataFrame:
    """
    Fetch benchmark prices from yfinance and shape them to match the Snowflake table schema.
    - Returns a DataFrame with columns:
      ["BENCHMARKCODE","PERFORMANCEDATATYPE","CURRENCYCODE","CURRENCY",
       "PERFORMANCEFREQUENCY","VALUE","HISTORYDATE1","HISTORYDATE"]
    """
    raw = yf.download(
        benchmark_ticker,
        start=start_date,
        end=end_date,
        interval="1d",
        progress=False,
        auto_adjust=True
    )
    if raw.empty:
        return pd.DataFrame(columns=[
            "BENCHMARKCODE","PERFORMANCEDATATYPE","CURRENCYCODE","CURRENCY",
            "PERFORMANCEFREQUENCY","VALUE","HISTORYDATE1","HISTORYDATE"
        ])

    price = raw["Close"]

    # Resample to the desired frequency if needed (default daily)
    if frequency != "D":
        price = price.resample(frequency).last()

    df = price.reset_index()
    df.columns = ["HISTORYDATE1", "VALUE"]

    # Keep date and timestamp strings to match target table types
    ts = pd.to_datetime(df["HISTORYDATE1"])
    df["HISTORYDATE1"] = ts.dt.strftime("%Y-%m-%d")           # date string
    df["HISTORYDATE"]  = ts.dt.strftime("%Y-%m-%d %H:%M:%S")  # timestamp string

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



################################################################
# --- add below to benchmark_fetcher.py ---
import datetime as dt

def build_benchmark_performance(
    tickers=("^GSPC", "AGG"),
    start_date="2000-01-01",
    end_date=None,
    frequency="D",
    save_csv_path=None,
) -> pd.DataFrame:
    """
    Fetch prices for multiple benchmarks and combine them into a single DataFrame
    matching the target table schema.

    Parameters
    ----------
    tickers : iterable of str
        Benchmark tickers, e.g., ("^GSPC", "AGG").
    start_date : str
        Start date (YYYY-MM-DD).
    end_date : str or None
        End date (YYYY-MM-DD). If None, uses today's date.
    frequency : str
        Resampling frequency. Default "D" (daily). Examples: "M" (month-end), "Q" (quarter-end).
    save_csv_path : str or None
        If provided, the combined result will also be saved to this CSV path.

    Returns
    -------
    pandas.DataFrame
        Columns: ["BENCHMARKCODE", "PERFORMANCEDATATYPE", "CURRENCYCODE", "CURRENCY",
                  "PERFORMANCEFREQUENCY", "VALUE", "HISTORYDATE1", "HISTORYDATE"].
    """
    if end_date is None:
        end_date = dt.date.today().isoformat()

    frames = []
    for t in tickers:
        df = get_benchmark_performance(
            benchmark_ticker=t,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )
        if not df.empty:
            frames.append(df)

    if frames:
        out = pd.concat(frames, ignore_index=True)
    else:
        out = pd.DataFrame(columns=[
            "BENCHMARKCODE","PERFORMANCEDATATYPE","CURRENCYCODE","CURRENCY",
            "PERFORMANCEFREQUENCY","VALUE","HISTORYDATE1","HISTORYDATE"
        ])

    if save_csv_path:
        out.to_csv(save_csv_path, index=False)

    return out


if __name__ == "__main__":
    # Default run: fetch ^GSPC and AGG from 2000-01-01 to today (daily) and save to CSV.
    df_bench = build_benchmark_performance(
        tickers=("^GSPC", "AGG"),
        start_date="2000-01-01",
        end_date=dt.date.today().isoformat(),
        frequency="D",
        save_csv_path="benchmark_performance.csv"
    )
    print(df_bench.head())
    print(f"Rows: {len(df_bench)} | Saved to: benchmark_performance.csv")


