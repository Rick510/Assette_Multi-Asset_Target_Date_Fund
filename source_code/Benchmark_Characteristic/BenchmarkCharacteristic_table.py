import time
import logging
from datetime import date
from typing import Optional, List
import pandas as pd
import yfinance as yf
from IPython.display import display
from dateutil.relativedelta import relativedelta

# ---- display config: disable scientific notation ----
pd.set_option("display.float_format", lambda x: f"{x:.6f}" if pd.notna(x) and isinstance(x, float) else x)

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("benchmark_perf")

# ---- constants ----
CHARACTERISTICS = [
    "# of Securities",
    "Price/Earnings (TTM)",
    "Price/Earnings (Forward)",
    "Price/Book Value",
    "Price/Sales (TTM)",
    "Dividend Yield",
    "Dividends Per Share",
    "EPS LTM",
    "EPS Growth (YoY)",
    "Return On Equity",
    "1-Year Return (%)",
    "5-Year Return (%)",
    "Market Cap"
]

ETF_PROXIES = ["SPY", "IVV", "VOO"]  # fallback universe for index metrics
DELAY_BETWEEN_BMARKS = 1.0  # throttle between benchmark processing

CURRENCY_NAME_MAP = {
    "USD": "US Dollar",
    "EUR": "Euro",
    "GBP": "British Pound",
    "JPY": "Japanese Yen",
}

# caches
_info_cache: dict[str, dict] = {}
_constituent_cache: Optional[int] = None


# ---- yfinance helpers ----
def get_info(ticker: str) -> dict:
    if ticker in _info_cache:
        return _info_cache[ticker]
    try:
        tkr = yf.Ticker(ticker)
        info = tkr.info or {}
    except Exception as e:
        logger.warning(f"{ticker} info fetch failed: {e}")
        info = {}
    _info_cache[ticker] = info
    return info


def get_price(ticker: str) -> Optional[float]:
    info = get_info(ticker)
    price = info.get("regularMarketPrice") or info.get("previousClose")
    if price is not None:
        return price
    try:
        hist = yf.Ticker(ticker).history(period="5d", interval="1d")
        if hist is not None and "Close" in hist and len(hist) > 0:
            return hist["Close"].iloc[-1]
    except Exception as e:
        logger.warning(f"{ticker} fallback history price failed: {e}")
    return None


def get_return(ticker: str, years: int) -> Optional[float]:
    """
    Annualized return over `years` using Close price.
    """
    try:
        end_date = pd.Timestamp("today").normalize()
        start_date = end_date - relativedelta(years=years)
        hist = yf.Ticker(ticker).history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            interval="1d",
        )
        if hist is None or "Close" not in hist or len(hist) < 2:
            return None
        close = hist["Close"]
        start = close.iloc[0]
        end = close.iloc[-1]
        if start <= 0 or end is None:
            return None
        total = end / start
        if years <= 1:
            ann = total - 1
        else:
            ann = total ** (1 / years) - 1
        return ann * 100
    except Exception as e:
        logger.warning(f"{ticker} {years}y return failed: {e}")
        return None


# ---- characteristic-specific computation with fallback across tickers ----
def compute_pe_ttm(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        price = get_price(t)
        trailing_eps = info.get("trailingEps")
        if price is not None and trailing_eps not in (None, 0):
            try:
                return price / trailing_eps
            except Exception:
                pass
        if info.get("trailingPE") is not None:
            return info.get("trailingPE")
    return None


def compute_forward_pe(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        price = get_price(t)

        if info.get("forwardPE") is not None:
            return info.get("forwardPE")

        forward_eps = info.get("forwardEps")
        if price is not None and forward_eps not in (None, 0):
            try:
                return price / forward_eps
            except Exception:
                pass

        trailing_eps = info.get("trailingEps")
        growth = info.get("earningsQuarterlyGrowth")
        if price is not None and trailing_eps not in (None, 0) and growth not in (None,):
            try:
                est_forward_eps = trailing_eps * (1 + growth)
                if est_forward_eps != 0:
                    return price / est_forward_eps
            except Exception:
                pass
    return None


def compute_price_to_book(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("priceToBook") is not None:
            return info.get("priceToBook")
        price = get_price(t)
        book_value = info.get("bookValue")
        if price is not None and book_value not in (None, 0):
            try:
                return price / book_value
            except Exception:
                pass
    return None


def compute_price_to_sales(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("priceToSalesTrailing12Months") is not None:
            return info.get("priceToSalesTrailing12Months")
        market_cap = info.get("marketCap")
        revenue = info.get("totalRevenue") or info.get("revenueTrailing12Months")
        if market_cap not in (None,) and revenue not in (None, 0):
            try:
                return market_cap / revenue
            except Exception:
                pass
    return None


def compute_dividend_yield(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("dividendYield") is not None:
            return info.get("dividendYield")
        dividend_rate = info.get("dividendRate") or info.get("trailingAnnualDividendRate")
        price = get_price(t)
        if dividend_rate not in (None,) and price not in (None, 0):
            try:
                return dividend_rate / price
            except Exception:
                pass
    return None


def compute_dividends_per_share(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("dividendRate") is not None:
            return info.get("dividendRate")
        if info.get("trailingAnnualDividendRate") is not None:
            return info.get("trailingAnnualDividendRate")
    return None


def compute_eps_ltm(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("trailingEps") is not None:
            return info.get("trailingEps")
    return None


def compute_eps_growth(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("earningsQuarterlyGrowth") is not None:
            return info.get("earningsQuarterlyGrowth")
    return None


def compute_roe(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("returnOnEquity") is not None:
            return info.get("returnOnEquity")
    return None


def compute_market_cap(tickers: List[str]) -> Optional[float]:
    for t in tickers:
        info = get_info(t)
        if info.get("marketCap") is not None:
            return info.get("marketCap")
        price = get_price(t)
        shares = info.get("sharesOutstanding")
        if price not in (None,) and shares not in (None,):
            try:
                return price * shares
            except Exception:
                pass
    return None


def get_sp500_constituent_count() -> int:
    global _constituent_cache
    if _constituent_cache is not None:
        return _constituent_cache

    def try_holdings(etf: str) -> Optional[int]:
        tkr = yf.Ticker(etf)
        for attr in ("holdings", "fund_holdings", "get_holdings"):
            try:
                candidate = getattr(tkr, attr)
                df = candidate() if callable(candidate) else candidate
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return len(df)
            except Exception:
                continue
        return None

    count = None
    for etf in ETF_PROXIES:
        count = try_holdings(etf)
        if count is not None:
            break

    if count is None:
        logger.warning("Falling back to 503 for S&P 500 constituent count")
        count = 503
    try:
        count = int(count)
    except Exception:
        pass
    _constituent_cache = count
    return count


# ---- orchestrator ----
def build_benchmark_characteristics_table(benchmark_map: dict[str, str]) -> pd.DataFrame:
    rows = []
    for code, primary_ticker in benchmark_map.items():
        logger.info(f"Building characteristics for {code} ({primary_ticker})")
        as_of = date.today()
        tickers = [primary_ticker] + ETF_PROXIES  # primary then fallbacks

        currency = get_info(primary_ticker).get("currency") or "USD"
        currency_name = CURRENCY_NAME_MAP.get(currency, currency)

        values = {
            "# of Securities": get_sp500_constituent_count() if code.lower().startswith("sp500") else None,
            "Price/Earnings (TTM)": compute_pe_ttm(tickers),
            "Price/Earnings (Forward)": compute_forward_pe(tickers),
            "Price/Book Value": compute_price_to_book(tickers),
            "Price/Sales (TTM)": compute_price_to_sales(tickers),
            "Dividend Yield": compute_dividend_yield(tickers),
            "Dividends Per Share": compute_dividends_per_share(tickers),
            "EPS LTM": compute_eps_ltm(tickers),
            "EPS Growth (YoY)": compute_eps_growth(tickers),
            "Return On Equity": compute_roe(tickers),
            "1-Year Return (%)": get_return(primary_ticker, 1) or next((get_return(etf, 1) for etf in ETF_PROXIES if get_return(etf, 1) is not None), None),
            "5-Year Return (%)": get_return(primary_ticker, 5) or next((get_return(etf, 5) for etf in ETF_PROXIES if get_return(etf, 5) is not None), None),
            "Market Cap": compute_market_cap(tickers),
        }

        for name in CHARACTERISTICS:
            val = values.get(name)
            if name == "# of Securities" and val is not None:
                try:
                    val = int(val)
                except Exception:
                    pass
            rows.append({
                "BENCHMARKCODE":             code,
                "CURRENCYCODE":              currency,
                "CURRENCY":                  currency_name,
                "LANGUAGECODE":              "en-US",
                "CATEGORY":                  "Total",
                "CATEGORYNAME":              None,
                "CHARACTERISTICNAME":        name,
                "CHARACTERISTICDISPLAYNAME": name,
                "STATISTICTYPE":             "NA",
                "CHARACTERISTICVALUE":       val,
                "ABBREVIATEDTEXT":           None,
                "HISTORYDATE":               as_of
            })

        time.sleep(DELAY_BETWEEN_BMARKS)
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["CHARACTERISTICVALUE"])
    return df


# ---- execution ----
if __name__ == "__main__":
    benchmark_map = {"sp500": "^GSPC"}
    df_char = build_benchmark_characteristics_table(benchmark_map)
    display(df_char)

'''
Right now, the BenchmarkCharacteristic table we generated 
has not been uploaded to Snowflake yet — we have only saved it as a CSV file
called BenchmarkCharacteristic.csv, which can also found in the same folder.
'''
# df_char.to_csv()