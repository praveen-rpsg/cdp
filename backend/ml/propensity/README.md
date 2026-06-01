# Customer Propensity Model — Spencer's & NBL

Multi-label propensity scoring across product segments.  Trains **N independent binary classifiers** (one per segment), then normalizes the raw probabilities to a valid distribution per customer.

---

## Segments

| Brand | # | Segment |
|---|---|---|
| Spencer's | 1 | FASHION CB |
| Spencer's | 2 | FOOD |
| Spencer's | 3 | GM |
| Spencer's | 4 | HI TECH |
| Spencer's | 5 | NON TRADE |
| Spencer's | 6 | NON FOOD GROCERY |
| NBL | 1 | FOOD |
| NBL | 2 | GM |
| NBL | 3 | NON TRADE |
| NBL | 4 | NON FOOD GROCERY |

---

## Module layout

```
ml/propensity/
  config.py        — date scope, segment lists, output column names, LGBM/LR hyper-params
  data_prep.py     — SQL extraction (labels + features) from the DWH
  features.py      — feature engineering, preprocessing (LGBM-native / LR pipeline)
  trainer.py       — StratifiedKFold CV + final refit for each segment model
  evaluator.py     — AUC, PR-AUC, Log Loss, Precision, Recall, F1
  normalizer.py    — per-customer probability normalization
  pipeline.py      — end-to-end orchestration + CLI entry point
  schema.sql       — DDL for output tables
```

---

## Data scope

- **Date window:** 27-Jan-2026 → 06-Feb-2026 (configurable in `config.py`)
- **Source tables:**

| Alias | Spencer's | NBL |
|---|---|---|
| profiles | `silver_identity.unified_profiles` | `nb_silver_identity.unified_profiles` |
| behaviors | `silver_reverse_etl.customer_behavioral_attributes` | `nb_silver_reverse_etl.customer_behavioral_attributes` |
| transactions | `silver.s_fact_bill_transactions` | `nb_silver.s_fact_bill_transactions` |

---

## Label construction

A customer is labelled **positive for segment X** if they have ≥1 bill in the date window where `UPPER(TRIM(bt.segment_desc)) = 'X'` and the bill is not a sales return.

This is a **multi-label** setup — the same customer can be positive for multiple segments simultaneously.

---

## Feature groups

| Group | Source | Features |
|---|---|---|
| Demographics | `unified_profiles` | city_tier, gender, age, has_email, primary_source |
| Behavioral (pre-computed) | `customer_behavioral_attributes` | total_spend, total_visits, avg_basket, recency_days, RFM score, CLV, churn_risk |
| Overall RFM (window) | `s_fact_bill_transactions` | total_visits, total_spend, avg_basket, active_days, distinct_stores, promo_rate, weekend_rate, ecom_rate, discount_rate |
| Per-segment (window) | `s_fact_bill_transactions` | spend, visits, avg_qty per segment |
| Derived | computed | spend_share, visit_share, dominant_segment per segment |

---

## Modelling approach

### Algorithm: LightGBM (primary)
- Handles missing values via surrogate splits — no imputation needed
- Handles categoricals (city_tier, gender, rfm_segment) as `category` dtype
- Class imbalance handled via `scale_pos_weight = n_negatives / n_positives`
- Hyper-parameters tuned for retail propensity tasks (see `config.LGBM_PARAMS`)
- Early stopping on AUC against a held-out fold

### Benchmark: Logistic Regression
- Full sklearn pipeline: median imputation → StandardScaler → OHE → LR(C=0.1, balanced)
- Useful to sanity-check LGBM and detect data leakage

### Validation
- **Stratified 5-fold CV** on the labelled population
- OOF (out-of-fold) predictions collected for threshold-independent evaluation
- Final model retrained on 100% of the labelled data using the mean best iteration from CV

---

## Probability normalization

Raw scores from N independent binary models do **not** sum to 1.  After scoring:

```
Normalized_i = Raw_i / (Raw_1 + Raw_2 + ... + Raw_N)
```

If a customer has all-zero raw scores (no purchases at all in the window), scores are set to `1/N` (uniform).

Both raw and normalized scores are retained in the output.

---

## Output columns

### Spencer's
| Column | Type | Description |
|---|---|---|
| `SPENCERS_SEGMENT_1_PROPENSITY` | FLOAT | Raw probability — FASHION CB |
| `SPENCERS_SEGMENT_2_PROPENSITY` | FLOAT | Raw probability — FOOD |
| `SPENCERS_SEGMENT_3_PROPENSITY` | FLOAT | Raw probability — GM |
| `SPENCERS_SEGMENT_4_PROPENSITY` | FLOAT | Raw probability — HI TECH |
| `SPENCERS_SEGMENT_5_PROPENSITY` | FLOAT | Raw probability — NON TRADE |
| `SPENCERS_SEGMENT_6_PROPENSITY` | FLOAT | Raw probability — NON FOOD GROCERY |
| `SPENCERS_SEGMENT_*_NORMALIZED_PROPENSITY` | FLOAT | Normalized (sum = 1 per customer) |

### NBL
| Column | Type | Description |
|---|---|---|
| `NBL_SEGMENT_1_PROPENSITY` | FLOAT | Raw probability — FOOD |
| `NBL_SEGMENT_2_PROPENSITY` | FLOAT | Raw probability — GM |
| `NBL_SEGMENT_3_PROPENSITY` | FLOAT | Raw probability — NON TRADE |
| `NBL_SEGMENT_4_PROPENSITY` | FLOAT | Raw probability — NON FOOD GROCERY |
| `NBL_SEGMENT_*_NORMALIZED_PROPENSITY` | FLOAT | Normalized (sum = 1 per customer) |

---

## Usage

### Install dependencies
```bash
pip install -r ml/propensity/requirements.txt
```

### Run from CLI
```bash
# Spencer's — LightGBM, write CSV
python -m ml.propensity.pipeline \
    --brand spencers \
    --algo lgbm \
    --db-dsn "host=dwh-host dbname=cdp user=cdp password=secret" \
    --output-csv ./output/spencers_propensity.csv \
    --model-dir ./models

# NBL
python -m ml.propensity.pipeline \
    --brand natures_basket \
    --algo lgbm \
    --db-dsn "host=dwh-host dbname=cdp user=cdp password=secret" \
    --output-csv ./output/nbl_propensity.csv
```

### Run programmatically
```python
import psycopg
from ml.propensity.pipeline import run_pipeline

conn = psycopg.connect("host=dwh-host dbname=cdp user=cdp password=secret")
output_df = run_pipeline(
    brand_code   = "spencers",
    conn         = conn,
    algo         = "lgbm",
    output_csv   = "./output/spencers_propensity.csv",
    output_table = "silver_reverse_etl.customer_propensity_scores_spencers",
)
conn.close()
```

### Run both brands
```python
from ml.propensity.pipeline import run_all_brands

results = run_all_brands(
    conn_spencers = spencers_conn,
    conn_nbl      = nbl_conn,
    algo          = "lgbm",
    output_dir    = "./output",
)
```

---

## Create output tables (once)
```bash
psql -d cdp -f ml/propensity/schema.sql
```

---

## Evaluation metrics

All metrics computed per fold and averaged:

| Metric | Why |
|---|---|
| ROC-AUC | Threshold-independent ranking quality |
| PR-AUC | Better for imbalanced classes (focuses on positives) |
| Log Loss | Calibration quality of raw probabilities |
| Precision | Of customers predicted positive, how many truly are |
| Recall | Of truly positive customers, how many we capture |
| F1 | Harmonic mean of P/R at 0.5 threshold |

---

## Important notes

- **Do not combine brands** — models are trained independently; Spencer's and NBL have different customer populations, SKU hierarchies, and purchase patterns.
- **No future leakage** — all feature aggregations are strictly bounded by `end_date`.
- **Sales returns excluded** — `bt.sales_return IS DISTINCT FROM TRUE` in all queries.
- **Mobile join** — `RIGHT(bt.mobile_number, 10) = RIGHT(p.canonical_mobile, 10)` — consistent with the existing CDP compiler convention.
- **Re-run cadence** — Retrain monthly or when data shifts significantly (use PR-AUC drift as the trigger signal).
