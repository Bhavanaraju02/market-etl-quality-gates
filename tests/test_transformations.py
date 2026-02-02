import pandas as pd
from etl.transform import clean_prices

def test_clean_prices_drops_nulls_and_invalids():
    df = pd.DataFrame([
        {"symbol":"AAPL.US","date":"2024-01-01","open":1,"high":2,"low":1,"close":2,"volume":10},
        {"symbol":"AAPL.US","date":"2024-01-02","open":None,"high":2,"low":1,"close":2,"volume":10},
        {"symbol":"AAPL.US","date":"2024-01-03","open":1,"high":0,"low":1,"close":2,"volume":10},
    ])
    out = clean_prices(df)
    assert len(out) == 1
