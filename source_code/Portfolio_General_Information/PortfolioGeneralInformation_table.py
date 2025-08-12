# -*- coding: utf-8 -*-


import pandas as pd
import random
random.seed(42) 
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from io import StringIO
from datetime import date
from IPython.display import display, Markdown
import yfinance as yf
from babel.numbers import get_currency_name


target_date_funds = ['VSVNX','VLXVX','VTTSX','VFFVX','VFIFX', 'VTIVX','VFORX','VTTHX','VTHRX','VTTVX','VTWNX','VTINX']


# Generate PRD001–PRD012 list
product_codes = [f"PRD{i:03d}" for i in range(1, 13)]  # 1 to 12


import random
import pandas as pd
from datetime import date
import yfinance as yf
from babel.numbers import get_currency_name

# Make sure these two are defined somewhere in your module or passed in:
# product_codes = [...]
# target_date_funds = [{"TICKER": "VBTIX"}, {"TICKER": "VTINX"}, …]

def generate_portfolio_general(num_portfolios: int) -> pd.DataFrame:
    df_general_all = []

    for idx in range(num_portfolios):
        portfolio_code = f"PORT{idx+1:03d}"
        product_code   = random.choice(product_codes)

        name     = f"Retirement Portfolio {idx+1}"
        style    = "Growth"
        category = "Individual Account"

        # pick 4 random funds
        selected = random.sample(target_date_funds, k=4)

        # derive currency code from first fund
        info = yf.Ticker(selected[0]).info
        currency_code = info.get("currency", "USD")
        try:
            currency_name = get_currency_name(currency_code, locale="en")
        except:
            currency_name = currency_code

        # fetch each fund's inception date and pick the earliest
        inception_dates = []
        for fund in selected:
            raw = yf.Ticker(fund).info.get("fundInceptionDate")
            if isinstance(raw, (int, float)):
                inception_dates.append(pd.to_datetime(raw, unit="s").date())
        open_date = min(inception_dates) if inception_dates else date.today()

        df_general_all.append({
            "BASECURRENCYCODE":         currency_code,
            "BASECURRENCYNAME":         currency_name,
            "INVESTMENTSTYLE":          style,
            "ISBEGINOFDAYPERFORMANCE":  True,
            "NAME":                     name,
            "OPENDATE":                 open_date,
            "PERFORMANCEINCEPTIONDATE": open_date,
            "PORTFOLIOCATEGORY":        category,
            "PORTFOLIOCODE":            portfolio_code,
            "PRODUCTCODE":              product_code,
            "TERMINATIONDATE":          None
        })

    return pd.DataFrame(df_general_all)


if __name__ == "__main__":
    # quick test when running this script directly
    df_general = generate_portfolio_general(10)
    print(df_general)


'''
Right now, the PortfolioGeneralInformation table we generated 
has not been uploaded to Snowflake yet — we have only saved it as a CSV file
called PortfolioGeneralInformation.csv, which can also found in the same folder.
'''
# df_general.to_csv()


