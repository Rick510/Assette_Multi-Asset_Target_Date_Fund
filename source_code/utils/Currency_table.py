
import pandas as pd
import requests

def get_country_currency_df(exclude_regions=None):
    """
    Fetches country and currency info from the REST Countries API
    and returns it as a pandas DataFrame.

    Parameters
    ----------
    exclude_regions : set of str, optional
        Region names (lowercase) to skip entirely (e.g. {"antarctic"}).

    Returns
    -------
    pandas.DataFrame
        Columns: country_name, country_code, currency_name,
                 currency_code, region, subregion
    """
    if exclude_regions is None:
        exclude_regions = {"antarctic"}

    endpoint = (
        "https://restcountries.com/v3.1/all"
        "?fields=name,currencies,cca2,region,subregion"
    )
    resp = requests.get(endpoint, timeout=10)
    resp.raise_for_status()
    raw = resp.json()

    records = []
    for country in raw:
        region_key = (country.get("region") or "").strip().lower()
        if region_key in exclude_regions:
            continue

        for code, info in country.get("currencies", {}).items():
            name = country.get("name", {}).get("common")
            ccode = country.get("cca2")
            cname = info.get("name")
            if name and ccode and cname and code:
                records.append({
                    "country_name":   name,
                    "country_code":   ccode,
                    "currency_name":  cname,
                    "currency_code":  code,
                    "region":         country.get("region"),
                    "subregion":      country.get("subregion")
                })

    return pd.DataFrame.from_records(records)

if __name__ == "__main__":
    df_currency = get_country_currency_df()
    print(df_currency)


'''
Right now, the Currency table we generated 
has not been uploaded to Snowflake yet — we have only saved it as a CSV file
called Currency.csv, which can also found in the same folder.
'''
# df_currency.to_csv()