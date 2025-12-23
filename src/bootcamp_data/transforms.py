import pandas as pd
import re

# this function will make sure that the fileds has the right data types
def enforce_schema(df) -> pd.DataFrame:
    return df.assign(amount = pd.to_numeric(df["amount"],errors="coerce").astype("Float64"),
              quantity = pd.to_numeric(df["quantity"],errors="coerce").astype("Int64"),
              order_id = df["order_id"].astype("string"),
               user_id = df["user_id"].astype("string"))

# this function will create to columns the number of missing values and the percentage of missing values
def missingness_report(df):
    n = len(df)
    return ( df.isna().sum().rename("n_missing").to_frame().assign(p_missing=lambda t: t["n_missing"] / n)
        .sort_values("p_missing", ascending=False))

# this function will flag each for for missing value if there is NA it will be true
def add_missing_flags(df, cols):
    out = df.copy()
    for c in cols:
        print(c)
        out[f"{c}__isna"] = out[c].isna()
    return out

_ws = re.compile(r"\s+")

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))

# this function will remove duplicates and keep the last according to the timestamp
def dedupe_keep_latest(df, key_cols, ts_col) -> pd.DataFrame:
    return df.sort_values(ts_col).drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
    
#this function if the column date is string will conert it into datetime datatype
def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

# this function will add the column timestamp to the copied dataframe
def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    ts = df[ts_col]
    return df.assign( date=ts.dt.date, year=ts.dt.year, month=ts.dt.to_period("M").astype("string"),
    dow=ts.dt.day_name(), hour=ts.dt.hour, )

# this function will calculate the iqr
def iqr_bounds(s, k=1.5):
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k*iqr), float(q3 + k*iqr)

def winsorize(s, lo=0.01, hi=0.99):
    a, b = s.quantile(lo), s.quantile(hi)
    return s.clip(lower=a, upper=b)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})