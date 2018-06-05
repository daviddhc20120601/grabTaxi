import pandas as pd


def to_df(path):
    order_df = pd.read_csv(path, index_col=False)
    return order_df
