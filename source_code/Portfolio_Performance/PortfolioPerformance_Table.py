from datetime import date
import numpy as np
import pandas as pd
import yfinance as yf
from yahooquery import Ticker
import random
from datetime import date, datetime, timedelta



########################################################
########################################################
import pandas as pd
from datetime import date, datetime, timedelta
from yahooquery import Ticker
import yfinance as yf

# 常setup the variables first
_TICKERS = [
    'VSVNX','VLXVX','VTTSX','VFFVX','VFIFX',
    'VTIVX','VFORX','VTTHX','VTHRX','VTTVX',
    'VTWNX','VTINX'
]
_PERF_INCEP = datetime(2004, 12, 1)  # set the startdate to fetch data

def generate_performance_factors() -> pd.DataFrame:
    """
    Calculate  daily Fund Ticker(Product) Gross / Net Return from the date we set to today,
    and save it to a dataframe to merge portfoliocode later.
    """
    # Fetch the data to today
    today = date.today()

    # Get expenseRatio to calculate Net Return later
    tk = Ticker(_TICKERS)
    profiles = tk.get_modules('fundProfile')
    expense_ratio = {
        t: profiles.get(t, {}) \
                    .get('fundProfile', {}) \
                    .get('expenseRatio', 0.0)
        for t in _TICKERS
    }

    # Download history Price based on the duration we set above
    raw = yf.download(
        _TICKERS,
        start=_PERF_INCEP,
        end=today + timedelta(days=1),
        interval='1d',
        group_by='ticker',
        auto_adjust=False,
        actions=True,
        progress=False
    )

    # calculate sliding window price
    records = []
    for t in _TICKERS:
        if t not in raw.columns.get_level_values(0):
            continue
        df_t = raw[t].copy().sort_index()
        if df_t.empty:
            continue
        df_t.index = pd.to_datetime(df_t.index)

        for i in range(1, len(df_t)):
            d0, d1 = df_t.index[i-1], df_t.index[i]
            price0 = df_t.loc[d0, 'Close']
            price1 = df_t.loc[d1, 'Close']
            gross = price1 / price0 - 1

            days = (d1 - d0).days
            net = gross - expense_ratio.get(t, 0.0) * days / 365

            records.append({
                'FUND TICKER':              t,
                'PERFORMANCEINCEPTIONDATE': d0.date(),
                'HISTORYDATE':              d1.date(),
                'PERFORMANCETYPE':          'Portfolio Gross',
                'PERFORMANCEFACTOR':         gross
            })
            records.append({
                'FUND TICKER':              t,
                'PERFORMANCEINCEPTIONDATE': d0.date(),
                'HISTORYDATE':              d1.date(),
                'PERFORMANCETYPE':          'Portfolio Net',
                'PERFORMANCEFACTOR':         net
            })

    df_performance_dict = pd.DataFrame.from_records(records)
    return df_performance_dict




######################################################



import pandas as pd
import random
from datetime import date, timedelta
from yahooquery import Ticker

from source_code.Holding_Details.HoldingDetails_Table import get_df_merged

## This function is to generate a random inception date for each account
def random_date(start: date, end: date) -> date:
    """Choose a random day from start to end date"""
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def generate_portfolio_performance(
    start_date: date = date(2024, 12, 1),
    end_date:   date = date(2025, 1, 31),
    seed:       int  = 42
) -> pd.DataFrame:
    """
    This function ain to merging portfolio and product performance.
    I randomly generate inceptiondate for each account and filter 
    the record that performance inception date prior to portfolio inception
    and return final df_portfolio_performance
    """
    # set a fixed random seed
    random.seed(seed)

    # Load holding data 
    df_merged = get_df_merged()
    df_hold = df_merged[[
        'PORTFOLIOCODE', 'TICKER', 'ISSUEDISPLAYNAME',
        'CURRENCYCODE', 'ISSUETYPE', 'PRICE', 'ASSETCLASSNAME',
        'QUANTITY_PRODUCT', 'HISTORYDATE', 'FUND TICKER', 'PRODUCTCODE'
    ]].copy()

    # Get performance factors
    df_performance_dict = generate_performance_factors()

    # create left table to merge later
    records = []
    for (pf, ft), g in df_hold.groupby(['PORTFOLIOCODE','FUND TICKER']):
        records.append({
            'PORTFOLIOCODE':           pf,
            'FUND TICKER':             ft,
            'CURRENCYCODE':            g['CURRENCYCODE'].iloc[0],
            'CURRENCY':                'US Dollar',
            'PERFORMANCECATEGORY':     'Asset Class',
            'PERFORMANCECATEGORYNAME': 'Total Portfolio',
            'PORTFOLIOINCEPTIONDATE':  random_date(start_date, end_date),
            'PERFORMANCEFREQUENCY':    'D',
        })
    df_left = pd.DataFrame(records)

    # Merge performance factors
    df_join = df_left.merge(
        df_performance_dict,
        on='FUND TICKER',
        how='left'
    )

    # set the column sequence
    cols = [
        'PORTFOLIOCODE','HISTORYDATE','CURRENCYCODE','CURRENCY',
        'PERFORMANCECATEGORY','PERFORMANCECATEGORYNAME','PERFORMANCETYPE',
        'PERFORMANCEINCEPTIONDATE','PORTFOLIOINCEPTIONDATE',
        'PERFORMANCEFREQUENCY','PERFORMANCEFACTOR'
    ]
    df_join = df_join[cols]

    # 6. only keep the column that performance inception >= portfolio inception 
    df_portfolio_performance = (
        df_join[df_join['PERFORMANCEINCEPTIONDATE'] >= df_join['PORTFOLIOINCEPTIONDATE']]
        .reset_index(drop=True)
    )

    return df_portfolio_performance

# Running this script directly will print out df_portfolio_performance
if __name__ == '__main__':
    df_portfolio_performance = generate_portfolio_performance()
    print(df_portfolio_performance)


'''
Right now, the PortfolioPerformance table we generated 
has not been uploaded to Snowflake yet — we have only saved it as a CSV file
called PortfolioPerformance.csv, which can also found in the same folder.
'''
# df_portfolio_performance.to_csv()

