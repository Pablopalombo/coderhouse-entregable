from scripts.utils import *


def load_stocks(key):
    base_url="http://api.marketstack.com/v1/tickers"
    endpoint="YPF/eod/latest"
    df = get_data(base_url, endpoint, key)
    engine = connect_to_db("config/config.ini","redshift")
    load_to_sql(df, engine)
