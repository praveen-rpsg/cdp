# NEXUS CDP — Spencer's Customer Data Platform

A composable Customer Data Platform built for RPSG Group's Spencer's Retail.
Handles identity resolution across 20M+ CIH profiles and POS transactions,
builds behavioral/lifecycle segments per the Resulticks attribute design,
and exposes a visual Segment Builder UI for marketing activation.

## Architecture

```
 Raw Sources                Bronze (PostgreSQL)           dbt Transformations              Application
 ───────────                ───────────────────           ───────────────────              ───────────
 ┌──────────────┐           ┌──────────────────┐          ┌──────────────────┐            ┌──────────────┐
 │ BILL_DELTA   │──zip──►   │ raw_bill_delta   │──►       │ staging views    │            │  React UI    │
 │ (7 days POS) │           │ 1.96M rows       │          │ stg_bill_*       │            │  :3000       │
 ├──────────────┤           ├──────────────────┤          ├──────────────────┤            ├──────────────┤
 │ CIH Parquet  │──parquet► │ raw_cih_profiles │──►       │ silver           │──►         │  FastAPI     │
 │ (20M golden) │           │ 20M rows         │          │ int_identity_*   │            │  :8000       │
 ├──────────────┤           ├──────────────────┤          ├──────────────────┤            ├──────────────┤
 │ Location     │──pipe──►  │ raw_location_*   │──►       │ silver_identity  │──►         │  Segment     │
 │ Promo_SRL    │──csv───►  │ raw_promotions   │          │ unified_profiles │            │  Builder     │
 │ ZABM         │──pipe──►  │ raw_article_*    │          │ identity_edges   │            │              │
 │ NPS / YVM    │──xlsx──►  │ raw_nps / yvm    │          ├──────────────────┤            └──────────────┘
 │ Cashback     │──xlsx──►  │ raw_promo_cash.  │          │ silver_gold      │
 └──────────────┘           └──────────────────┘          │ customer_txn_*   │
                                                          │ customer_channel │
                                                          │ customer_product │
                                                          ├──────────────────┤
                                                          │ silver_reverse_  │
                                                          │ etl              │
                                                          │ behavioral_attrs │
                                                          └──────────────────┘
```

## Data Model Layers

| Schema | Layer | Description | Key Tables |
|--------|-------|-------------|------------|
| `bronze` | Raw | Ingested as-is from source files | `raw_bill_delta`, `raw_cih_profiles`, `raw_location_master`, `raw_article_master` |
| `staging` | Staging | dbt views — cleaned, typed, derived flags | `stg_bill_transactions`, `stg_bill_identifiers`, `stg_bill_summary`, `stg_cih_profiles` |
| `silver` | Intermediate | Identity spine & resolution | `int_identity_spine`, `int_identity_resolved` |
| `silver_identity` | Identity | Unified profiles & identity graph | `unified_profiles` (20M+), `identity_edges`, `identity_graph_summary` |
| `silver_gold` | Gold | Customer-level aggregated metrics | `customer_transaction_summary`, `customer_channel_summary`, `customer_product_summary` |
| `silver_reverse_etl` | Reverse ETL | Activation-ready behavioral attributes | `customer_behavioral_attributes` (RFM, L1/L2 segments, lifecycle, deciles) |

## Identity Resolution

- **CIH** (Customer Identity Hub): 20M+ golden records, `brand_id` = mobile number
- **POS**: Real mobile numbers from BILL_DELTA transactions
- **Join key**: Mobile number (both sources)
- **Surrogate ID**: `MD5(mobile)` — same mobile across CIH and POS = same person
- **Result**: CIH-only, POS-only, and CIH+POS merged profiles in `unified_profiles`

## Resulticks Attribute Design

| Category | Attributes | Examples |
|----------|-----------|---------|
| **L1 Segment** | Store-type-aware value/frequency | HVHF, LVHF, HVLF, LVLF |
| **L2 Segment** | Lifecycle + recency-based | STAR, LOYAL, Win Back, New, ACTIVE, Inactive, LAPSER, Deep Lapsed |
| **Deciles** | Spend & NOB deciles (1-10) | `spend_decile`, `nob_decile` |
| **Derived Flags** | Temporal and channel flags | `weekend_flag`, `wednesday_flag`, `first_week_flag`, `delivery_channel` |
| **RFM** | Recency, Frequency, Monetary (1-5 each) | `rfm_recency_score`, `rfm_frequency_score`, `rfm_monetary_score` |
| **Favourites** | Top store, day, articles by spend/NOB | `fav_store_code`, `fav_day`, `fav_article_by_spend` |
| **Channel** | Online/Offline/Omni presence | `channel_presence`, `store_spend`, `online_spend` |

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | PostgreSQL 16 | Bronze/Silver/Gold layers + metadata |
| **Transformations** | dbt-core 1.7 | Identity resolution, aggregation, behavioral attributes |
| **Backend** | Python 3.11+ / FastAPI | API, segment compiler, query engine |
| **Frontend** | React 18 + TypeScript + Vite | Segment Builder UI |
| **Cache** | Redis 7 | Audience counts, session management |
| **Ingestion** | Python (psycopg v3, pandas, pyarrow) | Zip extraction, CSV/Parquet loading |
| **Container** | Docker Compose | Full-stack orchestration |

---

## Quick Start (Docker)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Docker Compose v2)
- ~8 GB free RAM (CIH parquet loading is memory-intensive)
- Raw data files on local disk (see [Data Sources](#data-sources) below)

### 1. Clone & Start Services

```bash
git clone https://github.com/rashdevesh/cdp.git
cd cdp

# Start all 4 services (Postgres, Redis, Backend, Frontend)
docker compose up -d
```

Verify everything is healthy:

```bash
docker compose ps
```

| Service | Port | URL |
|---------|------|-----|
| PostgreSQL | 5432 | `localhost:5432` (user: `cdp`, pass: `cdp`, db: `cdp_meta`) |
| Redis | 6379 | `localhost:6379` |
| Backend (FastAPI) | 8000 | http://localhost:8000 |
| Frontend (React) | 3000 | http://localhost:3000 |

### 2. Run Data Ingestion

The ingestion script loads all raw sources into the `bronze` schema:

```bash
# Install Python dependencies (one-time)
cd dwh/ingestion
pip install -r requirements.txt

# Run ingestion (loads BILL_DELTA zips, CIH parquet, Location, Promo, ZABM, NPS, YVM, etc.)
python ingest_spencers.py
```

**Environment variables** (optional — defaults work for local Docker):

| Variable | Default | Description |
|----------|---------|-------------|
| `RAW_DIR` | `D:\WORK\Sample Files\RAW\RAW` | Dated folders with BILL/Location/Promo/ZABM zips |
| `CIH_DIR` | `C:\Users\HP\Downloads\CIH_parquet\CIH_parquet` | 41 CIH parquet files |
| `SAMPLE_DIR` | `D:\WORK\Sample Files\01 Spencers\masked` | NPS, YVM, cashback, festival Excel files |
| `DATABASE_URL` | `postgresql://cdp:cdp@localhost:5432/cdp_meta` | PostgreSQL connection |

### 3. Run dbt Transformations

```bash
# Build all models (staging → intermediate → identity → gold → reverse ETL)
docker compose run --rm dbt run --profiles-dir . --full-refresh

# Run tests (22 tests across all layers)
docker compose run --rm dbt test --profiles-dir .
```

### 4. Open the App

- **Segment Builder UI**: http://localhost:3000
- **API Swagger Docs**: http://localhost:8000/docs
- **Attribute Catalog API**: http://localhost:8000/api/v1/segments/attributes/catalog?brand_code=spencers

### Data Sources

Place these files before running ingestion:

```
RAW_DIR/
├── 20260129/
│   ├── BILL_2026.01.29-*.zip          # ~96 store CSVs per zip
│   ├── Location_Master_*.csv.zip      # Pipe-delimited
│   ├── Promo_SRL_*.csv.zip            # Comma-delimited
│   └── ZABM_*.zip                     # Pipe-delimited, duplicate columns handled
├── 20260130/
│   └── ...
└── 20260206/
    └── ...

CIH_DIR/
├── part-00000-*.parquet               # 41 parquet files, ~20M profiles total
├── part-00001-*.parquet
└── ...

SAMPLE_DIR/
├── Spencers_NPS_*.xlsb
├── Spencers_promo_cashback_*.xlsx
├── festival_list_*.xlsx
├── Spencers_YVM_*.xlsx
├── ecom_category_*.xlsx
└── ecom_product_master_*.xlsx
```

---

## Common Operations

```bash
# Restart backend after code changes
docker compose restart backend

# Restart frontend after code changes
docker compose restart frontend

# Re-run dbt models
docker compose run --rm dbt run --profiles-dir . --full-refresh

# Run specific dbt model
docker compose run --rm dbt run --profiles-dir . --select customer_behavioral_attributes

# Check data counts
docker compose exec postgres psql -U cdp -d cdp_meta -c \
  "SELECT 'unified_profiles' AS tbl, COUNT(*) FROM silver_identity.unified_profiles
   UNION ALL SELECT 'txn_summary', COUNT(*) FROM silver_gold.customer_transaction_summary
   UNION ALL SELECT 'behavioral', COUNT(*) FROM silver_reverse_etl.customer_behavioral_attributes;"

# Stop everything (keeps data)
docker compose down

# Stop and delete all data
docker compose down -v
```

## Segment Builder — 84 Attributes Across 10 Categories

| Category | Count | Examples |
|----------|-------|---------|
| Identity | 16 | `customer_id`, `phone`, `email`, `surrogate_id`, `primary_source`, `identity_graph_size` |
| Demographics | 9 | `full_name`, `age`, `dob`, `customer_group`, `occupation` |
| Geographic | 8 | `city`, `pincode`, `region`, `state`, `zone`, `store_format` |
| Transactional | 17 | `total_spend`, `total_bills`, `spend_per_bill`, `avg_items_per_bill`, `promo_bill_count` |
| Behavioral | 5 | `recency_days`, `tenure_days`, `first_bill_date`, `last_bill_date`, `dgbt_fs` |
| Lifecycle | 10 | `l1_segment`, `l2_segment`, `lifecycle_stage`, `rfm_*_score`, `is_first_time_buyer` |
| Offline Store | 4 | `fav_store_code`, `fav_store_name`, `fav_store_type`, `fav_day` |
| Product Affinity | 6 | `fav_article_by_spend`, `fav_article_by_nob`, `second_fav_*` |
| Channel | 5 | `channel_presence`, `store_spend`, `online_spend`, `store_bills`, `online_bills` |
| Consent | 4 | `dnd`, `accepts_email_marketing`, `accepts_sms_marketing`, `gw_customer_flag` |

## 15 Pre-Built Segment Templates

Ready-to-use templates accessible from the Segment Builder UI:

- STAR Customers, HVHF Segment, At Risk High Spenders, Churned Win-Back
- New First-Time Buyers, Weekend Shoppers, Promo Lovers
- Omni-Channel, Online Only, Multi-Store Shoppers
- Top Spend Decile, Deep Lapsed, SMS Marketable, Repeat Buyers Trending Up
- CIH Registered No Purchase

---

## License

Internal RPSG Group use only.
