import pandas as pd

orders = pd.read_parquet("/Users/khaledalamro/Desktop/SDAIA_bootcamp/bootcamp_week2/week2-data-work/data/processed/orders.parquet")
# this function check for the column that you give if its not in the df it will say its missing
def require_columns(df, cols):
    missing = []
    for c in cols:
        if c not in df.columns:
            missing.append(c)
    assert not missing, f"Missing columns is: {missing}"

# this function check if the df is empty or not 
def assert_non_empty(df):
    assert len(df) > 0, "the Dataframe have 0 rows!"

#this function check if the primary key has null or duplicated values
def assert_unique_key(df, key, allow_na=False):
    if not allow_na and df[key].isna().any():
        raise ValueError(f"{key} contains NA")

    counts = df[key].value_counts() #here we will count the row vaules if one value come more than one it will count as duplicated
    if counts.max() > 1:
        raise ValueError(f"{key} is not unique.")


def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None:
    x = s.dropna()
    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"
    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"