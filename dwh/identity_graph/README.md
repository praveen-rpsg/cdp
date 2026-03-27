# Spencer's Identity Graph

## Architecture

The identity graph is built as a PostgreSQL-native graph stored in the `identity` schema:

- **`identity.unified_profiles`** — One row per resolved person (canonical node)
- **`identity.identity_edges`** — Edges linking identifiers to profiles
- **`identity.merge_log`** — Audit trail for identity merges

## Resolution Strategy

1. **Deterministic (Mobile):** Primary key for identity resolution. Mobile number
   is the strongest identifier available across CRM, NPS, YVM, and Promo systems.

2. **Deterministic (Email):** Secondary identifier from YVM feedback system.
   Linked to mobile during resolution.

3. **Probabilistic (Name):** Used for fuzzy matching when mobile is ambiguous.
   Lower confidence score (0.7).

4. **Behavioral (Store Affinity):** Tracks which stores a customer visits.
   Used for store-level attribution, not identity resolution.

## Graph Queries

See `queries.sql` for common graph traversal patterns.
