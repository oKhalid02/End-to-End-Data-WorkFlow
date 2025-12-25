
# ETL + EDA

## Key findings
- Total numeric revenue (ETL-processed): **145.50** (sum of numeric `amount` values).
- Orders by country (joined to `users.csv`): **SA: 4 orders (revenue 145.50)**.
- Refunds: **1 refund** (A0003) out of 5 total raw orders → **refund rate 20%**.
- Date range observed in `created_at`: **2025-12-01** to **2025-12-03** 

## Definitions
- **Revenue**: sum of `amount` across orders after numeric conversion and excluding non-numeric/invalid values.
- **Refund rate**: number of orders where `status_clean == "refund"` divided by total orders.
- **Time window**: min(`created_at`) to max(`created_at`) among parsable timestamps (here 2025-12-01 → 2025-12-03).

## Data quality caveats
- Missingness / parsing:
	- `amount`: one value could not be parsed as numeric (`"not_a_number"`) and was treated as missing for revenue calculations.
	- `quantity`: one missing value (blank) was observed and appears in the missingness report.
	- `created_at`: one record contains an invalid timestamp (`not_a_date`) and was excluded from time-based analyses.
- Case and value normalization:
	- `status` values vary in case (`Paid`, `paid`, `PAID`, `Refund`); cleaning must normalize case to compute refund/paid rates reliably.
- Duplicates and join coverage:
	- No duplicate `order_id` values were observed in the raw file.
	- All `user_id` values referenced by orders exist in `users.csv`, so join coverage is complete for this sample.
- Outliers / skew:
	- One relatively large amount (`100.00`) drives a large share of revenue; with only 4 numeric amounts, this creates high variance.

## Next questions / recommended follow-ups

- Add data validation rules to `src/bootcamp_data/quality.py` to: enforce numeric `amount`, require `quantity` non-null when applicable, and block malformed timestamps.
- Add unit tests for key transforms in `src/bootcamp_data/transforms.py` and `quality.py` to prevent regressions.

