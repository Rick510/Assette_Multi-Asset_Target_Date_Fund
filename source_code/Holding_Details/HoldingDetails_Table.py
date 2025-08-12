import pandas as pd
from io import StringIO
import numpy as np
from yahooquery import Ticker
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
import numpy as np
import pandas as pd
from datetime import date
from yahooquery import Ticker

# Define Fund ticker list first
tickers_list = ['VSVNX','VLXVX','VTTSX','VFFVX','VFIFX','VTIVX','VFORX','VTTHX','VTHRX','VTTVX','VTWNX','VTINX']

## Using this function, we can retrieve the holdings under each fund ticker.(For example, we can see the underlying holdings within the Vanguard 2050 Retirement Fund.)
# In this function output, fund represents Target Date Fund itself, sybol represents holdingdetails for each fund
def fetch_holdings_ticker(tickers: list[str]) -> pd.DataFrame:

    # ）Create Ticker query
    funds = Ticker(tickers)
    records = []

    for tk in tickers:

        info = funds.fund_holding_info.get(tk, {})
        holdings = info.get('holdings', [])
        for h in holdings:
            # add a column called symbol to note the source
            h['fund'] = tk
            records.append(h)

    return pd.DataFrame(records)


if __name__ == "__main__":
    tickers = tickers_list #['VSVNX', 'VLXVX', 'VTTSX', 'VFFVX', 'VFIFX','VTIVX', 'VFORX', 'VTTHX', 'VTHRX', 'VTTVX','VTWNX', 'VTINX']
    holdings_df = fetch_holdings_ticker(tickers)
    print(holdings_df)




## Import Portfolio General Information table
from source_code.Portfolio_General_Information.PortfolioGeneralInformation_table import generate_portfolio_general
df_general = generate_portfolio_general(10)



# Ticker to Product mapping
ticker_to_product = {
    'VTHRX': 'PRD001',
    'VTTSX': 'PRD002',
    'VTWNX': 'PRD003',
    'VTIVX': 'PRD004',
    'VTTHX': 'PRD005',
    'VLXVX': 'PRD006',
    'VFIFX': 'PRD007',
    'VFFVX': 'PRD008',
    'VTINX': 'PRD009',
    'VFORX': 'PRD010',
    'VTTVX': 'PRD011',
    'VSVNX': 'PRD012'
}
######################################################
## 
def create_holdings_dictionary(holdings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch data from YahooQuery and build a holdings dictionary DataFrame.
    """
    tickers = holdings_df['symbol'].unique().tolist()
    tk = Ticker(tickers, asynchronous=True)
    data = tk.get_modules(['defaultKeyStatistics', 'price', 'fundProfile'])

    records = []
    for _, row in holdings_df.iterrows():
        sym = row['symbol']
        fund = row['fund']
        weight = row['holdingPercent']
        stats = data.get(sym, {}).get('defaultKeyStatistics', {}) or {}
        price = data.get(sym, {}).get('price', {}) or {}
        profile = data.get(sym, {}).get('fundProfile', {}) or {}

        # Determine asset class
        cat = profile.get('categoryName', '').lower()
        if any(keyword in cat for keyword in ['bond', 'bill', 'note', 'income']):
            asset_cls = 'Fixed Income'
        elif any(keyword in cat for keyword in ['equity', 'stock', 'large blend']):
            asset_cls = 'Equity'
        else:
            asset_cls = 'Unknown'

        records.append({
            'PRODUCTCODE':    ticker_to_product.get(fund, pd.NA),
            'FUND TICKER':    fund,
            'TICKER':         sym,
            'holdingPercent': round(weight, 2),
            'ISSUEDISPLAYNAME': price.get('shortName'),
            'CURRENCYCODE':     price.get('currency'),
            'ISSUETYPE':        price.get('quoteType'),
            'PRICE':            price.get('regularMarketPreviousClose'),
            'ASSETCLASSNAME':   asset_cls,
            'HISTORYDATE':      date.today()
        })

    cols = [
        'PRODUCTCODE','TICKER','ISSUEDISPLAYNAME',
        'CURRENCYCODE','ISSUETYPE','PRICE','ASSETCLASSNAME',
        'FUND TICKER','holdingPercent','HISTORYDATE'
    ]
    return pd.DataFrame(records, columns=cols)






def generate_holdings_details(holdings_df: pd.DataFrame, num_portfolios: int = 10, seed: int = 42) -> pd.DataFrame:
    """
    Generate a merged holdings DataFrame with synthetic quantity, cost basis, and market value:
    1. Build holdings dictionary.
    2. Generate portfolio general info.
    3. Merge and calculate QUANTITY, COSTBASIS, MARKETVALUE.
    """
    # 1. Holdings dictionary
    holding_dict = create_holdings_dictionary(holdings_df)

    # 2. Generate portfolios
    df_left = df_general[['PORTFOLIOCODE', 'PRODUCTCODE']]
    df_merged = pd.merge(df_left, holding_dict, how='left', on='PRODUCTCODE')

    # 3. Synthetic data generation
    np.random.seed(seed)
    unique_codes = df_merged['PORTFOLIOCODE'].unique()
    quantity_map = {code: np.random.randint(500, 5001) for code in unique_codes}
    df_merged['QUANTITY_PRODUCT'] = df_merged['PORTFOLIOCODE'].map(quantity_map)
    df_merged['QUANTITY'] = df_merged['QUANTITY_PRODUCT'] * df_merged['holdingPercent']

    discount_rates = np.random.uniform(-0.1, 0.15, size=len(df_merged))
    df_merged['COSTBASIS'] = (df_merged['QUANTITY'] * df_merged['PRICE'] * (1 - discount_rates)).round(2)
    df_merged['MARKETVALUE'] = (df_merged['QUANTITY'] * df_merged['PRICE']).round(2)



    df_holdingdetails = df_merged[['PORTFOLIOCODE', 'TICKER', 'ISSUEDISPLAYNAME',
       'CURRENCYCODE', 'ISSUETYPE', 'PRICE', 'ASSETCLASSNAME', 'QUANTITY',
       'COSTBASIS', 'MARKETVALUE', 'HISTORYDATE']]

    return pd.DataFrame(df_holdingdetails)


## Package the above function into a module.
def main():

    tickers = ['VSVNX','VLXVX','VTTSX','VFFVX','VFIFX','VTIVX','VFORX','VTTHX','VTHRX','VTTVX','VTWNX','VTINX']  
    holdings_df = fetch_holdings_ticker(tickers)

    #generate holistic holding details 
    df_holdingdetails = generate_holdings_details(holdings_df, num_portfolios=10, seed=42)


    pd.set_option('display.max_rows', None)     # revise the display way
    print(df_holdingdetails)

if __name__ == "__main__":
    main()

'''
Right now, the Holdingdetails table we generated 
has not been uploaded to Snowflake yet — we have only saved it as a CSV file
called HoldingDetails.csv, which can also found in the same folder.
'''
# df_holdingdetails.to_csv()
##########################################################

def generate_merged_holdings(holdings_df: pd.DataFrame, num_portfolios: int = 10, seed: int = 42) -> pd.DataFrame:
    """
    Generate a merged holdings DataFrame with synthetic quantity, cost basis, and market value:
    1. Build holdings dictionary.
    2. Generate portfolio general info.
    3. Merge and calculate QUANTITY, COSTBASIS, MARKETVALUE.
    """
    # 1. Holdings dictionary
    holding_dict = create_holdings_dictionary(holdings_df)

    # 2. Generate portfolios
    df_left = df_general[['PORTFOLIOCODE', 'PRODUCTCODE']]
    df_merged = pd.merge(df_left, holding_dict, how='left', on='PRODUCTCODE')

    # 3. Synthetic data generation
    np.random.seed(seed)
    unique_codes = df_merged['PORTFOLIOCODE'].unique()
    quantity_map = {code: np.random.randint(500, 5001) for code in unique_codes}
    df_merged['QUANTITY_PRODUCT'] = df_merged['PORTFOLIOCODE'].map(quantity_map)
    df_merged['QUANTITY'] = df_merged['QUANTITY_PRODUCT'] * df_merged['holdingPercent']

    discount_rates = np.random.uniform(-0.1, 0.15, size=len(df_merged))
    df_merged['COSTBASIS'] = (df_merged['QUANTITY'] * df_merged['PRICE'] * (1 - discount_rates)).round(2)
    df_merged['MARKETVALUE'] = (df_merged['QUANTITY'] * df_merged['PRICE']).round(2)

    return pd.DataFrame(df_merged)


def get_df_merged(num_portfolios: int = 10, seed: int = 42) -> pd.DataFrame:
    holdings_df = fetch_holdings_ticker(tickers_list)
    
    return generate_merged_holdings(holdings_df, num_portfolios, seed)

