# Composable CDP — Multi-Brand Customer Data Platform

A SaaS composable Customer Data Platform that sits on any data warehouse/data lake.
Designed for multi-brand enterprises with brand-level identity resolution and
cross-brand linkage at the corporate BU level.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     Corporate BU (RPSG Group)                     │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐ │
│  │  Spencers    │ │    FMCG     │ │ Power CESC  │ │  Nature's  │ │
│  │ B2C/D2C/eCom│ │  D2C/B2B    │ │   B2C/B2B   │ │   Basket   │ │
│  └──────┬───────┘ └──────┬──────┘ └──────┬──────┘ └─────┬──────┘ │
│         │                │               │               │        │
│  ┌──────▼────────────────▼───────────────▼───────────────▼──────┐ │
│  │              Composable CDP Platform Layer                    │ │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌───────────┐ │ │
│  │  │  Identity   │ │ Segment    │ │  Audience  │ │  Activation│ │ │
│  │  │ Resolution  │ │  Engine    │ │  Builder   │ │   Layer    │ │ │
│  │  └────────────┘ └────────────┘ └────────────┘ └───────────┘ │ │
│  │  ┌────────────┐ ┌────────────┐ ┌────────────┐              │ │
│  │  │ Query      │ │ Profile    │ │ Consent &  │              │ │
│  │  │ Federation │ │ Unification│ │ Governance │              │ │
│  │  └────────────┘ └────────────┘ └────────────┘              │ │
│  └──────────────────────────┬───────────────────────────────────┘ │
│                             │                                     │
│  ┌──────────────────────────▼───────────────────────────────────┐ │
│  │            Data Lake Connector Layer (Federated)              │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │ │
│  │  │ Spencers │ │   FMCG   │ │   CESC   │ │ Nature's │       │ │
│  │  │AWS/Athena│ │AWS/Athena│ │AWS/Athena│ │AWS/Athena│       │ │
│  │  │Gold Layer│ │Gold Layer│ │Gold Layer│ │Gold Layer│       │ │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │ │
│  └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

## Brands

| Brand | Channels | Model |
|-------|----------|-------|
| Spencers | B2C, D2C, eCom | Retail hypermarket |
| FMCG | D2C, B2B | Consumer packaged goods |
| Power CESC | B2C, B2B | Energy/utilities |
| Nature's Basket | B2C, eCom | Premium grocery |

## Tech Stack

- **Backend**: Python 3.11+ / FastAPI
- **Frontend**: React 18 + TypeScript
- **Data Layer**: AWS Athena (federated over brand gold layers)
- **Metadata Store**: PostgreSQL
- **Cache**: Redis
- **Identity**: Brand-level graphs with corporate cross-brand linkage

## Quick Start

```bash
# Backend
cd backend && pip install -r requirements.txt
uvicorn app.main:app --reload

# Frontend
cd frontend && npm install && npm run dev
```
