from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
import json
import logging
import sys

ROOT = Path(__file__).resolve().parents[1] #this code make sure the moudle not foudn error is not happen
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.input_output import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import *
from bootcamp_data.joins import safe_left_join

@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw)
    assert_non_empty(users)
    assert_unique_key(users, "user_id")

    orders = enforce_schema(orders_raw)

    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))

    orders = orders.pipe(add_missing_flags, cols=["amount", "quantity"])

    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    joined = safe_left_join(orders, users, on="user_id", validate="many_to_one", suffixes=("", "_user"))
    assert len(joined) == len(orders), "Row count changed (join explosion?)"

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    return joined

log = logging.getLogger(__name__)

def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)
    # If you also want to write orders_clean.parquet, add it to outputs.

def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())

    meta = {
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")

def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    
    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics, users, cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, analytics=analytics)
