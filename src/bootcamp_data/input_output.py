from pathlib import Path
import pandas as pd

NA = ["", "NA", "N/A", "null", "None"]

def read_orders_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, na_values = NA, dtype={"order_id":"string","user_id":"string"})

def read_users_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, na_values=NA,dtype={"user_id":"string"})

def write_parquet(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)

def read_parquet(path):
    return pd.read_parquet(path)