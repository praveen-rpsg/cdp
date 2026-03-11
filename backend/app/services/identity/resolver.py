"""
Identity Resolution Service
=============================

Brand-level identity resolution with cross-brand linkage.

Architecture:
- Each brand owns its own identity graph
- Identity keys: email, phone, device_id, loyalty_id, external_ids
- Brand graphs are deterministic (exact match on keys)
- Corporate cross-brand linkage uses email_hash + phone as link keys
- Consent-gated: only links profiles with cross_brand_opt_in = True
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class IdentityKey:
    """A single identity key used for matching."""
    key_type: str  # 'email', 'phone', 'device_id', 'loyalty_id', 'external_id'
    key_value: str
    confidence: float = 1.0  # 1.0 = deterministic, <1.0 = probabilistic


@dataclass
class IdentityNode:
    """A node in the identity graph representing one identity."""
    brand_code: str
    customer_id: str
    identity_keys: list[IdentityKey]
    corporate_id: str | None = None  # Set when linked across brands


@dataclass
class LinkResult:
    """Result of a cross-brand linkage operation."""
    corporate_id: str
    linked_brand_profiles: list[dict]
    link_keys_used: list[str]
    confidence: float


class IdentityResolver:
    """
    Resolves and links customer identities within and across brands.

    Usage:
        resolver = IdentityResolver(athena_client=...)

        # Brand-level: merge duplicate profiles
        merged = await resolver.resolve_brand_identities("spencers")

        # Corporate: link across brands
        links = await resolver.link_cross_brand()
    """

    # Priority order for identity resolution
    KEY_PRIORITY = ["email", "phone", "loyalty_id", "device_id", "external_id"]

    # Deterministic match keys for cross-brand linkage
    CROSS_BRAND_LINK_KEYS = ["email_hash", "phone"]

    def __init__(self, athena_client: Any = None, db_session: Any = None):
        self.athena = athena_client
        self.db = db_session

    async def resolve_brand_identities(self, brand_code: str) -> dict:
        """
        Run brand-level identity resolution.

        Algorithm:
        1. Query all customers with their identity keys from gold layer
        2. Build an adjacency graph: nodes = customer_ids, edges = shared identity keys
        3. Find connected components — each component = one real customer
        4. Elect a "primary" customer_id per component (earliest created or most data)
        5. Write merged identity mapping table
        """
        logger.info(f"Running identity resolution for brand: {brand_code}")

        # SQL to extract identity keys from the brand's gold layer
        resolution_sql = f"""
        WITH identity_keys AS (
            SELECT
                customer_id,
                email,
                phone,
                COALESCE(loyalty_id, '') as loyalty_id,
                created_at
            FROM {brand_code}_gold.customers
            WHERE email IS NOT NULL OR phone IS NOT NULL
        ),
        -- Find clusters of customers sharing the same email
        email_clusters AS (
            SELECT
                LOWER(TRIM(email)) as match_key,
                ARRAY_AGG(DISTINCT customer_id) as customer_ids
            FROM identity_keys
            WHERE email IS NOT NULL AND email != ''
            GROUP BY LOWER(TRIM(email))
            HAVING COUNT(DISTINCT customer_id) > 1
        ),
        -- Find clusters of customers sharing the same phone
        phone_clusters AS (
            SELECT
                REGEXP_REPLACE(phone, '[^0-9]', '') as match_key,
                ARRAY_AGG(DISTINCT customer_id) as customer_ids
            FROM identity_keys
            WHERE phone IS NOT NULL AND phone != ''
            GROUP BY REGEXP_REPLACE(phone, '[^0-9]', '')
            HAVING COUNT(DISTINCT customer_id) > 1
        )
        SELECT 'email' as key_type, match_key, customer_ids FROM email_clusters
        UNION ALL
        SELECT 'phone' as key_type, match_key, customer_ids FROM phone_clusters
        """

        return {
            "brand_code": brand_code,
            "status": "resolution_query_compiled",
            "sql": resolution_sql,
        }

    async def link_cross_brand(self) -> dict:
        """
        Link customer profiles across brands at the corporate level.

        Algorithm:
        1. For each brand, extract consent-approved identity keys (email_hash, phone)
        2. Build a cross-brand adjacency graph
        3. Find connected components across brands
        4. Assign a corporate_id to each component
        5. Write corporate linkage table

        Only links profiles where cross_brand_opt_in = True.
        """
        logger.info("Running cross-brand identity linkage")

        linkage_sql = """
        WITH brand_keys AS (
            -- Extract linkage keys from each brand (consent-gated)
            SELECT
                'spencers' as brand_code,
                customer_id,
                TO_HEX(SHA256(TO_UTF8(LOWER(TRIM(email))))) as email_hash,
                REGEXP_REPLACE(phone, '[^0-9]', '') as phone_normalized
            FROM spencers_gold.customers
            WHERE cross_brand_opt_in = TRUE

            UNION ALL

            SELECT
                'fmcg' as brand_code,
                customer_id,
                TO_HEX(SHA256(TO_UTF8(LOWER(TRIM(email))))) as email_hash,
                REGEXP_REPLACE(phone, '[^0-9]', '') as phone_normalized
            FROM fmcg_gold.customers
            WHERE cross_brand_opt_in = TRUE

            UNION ALL

            SELECT
                'power_cesc' as brand_code,
                customer_id,
                TO_HEX(SHA256(TO_UTF8(LOWER(TRIM(email))))) as email_hash,
                REGEXP_REPLACE(phone, '[^0-9]', '') as phone_normalized
            FROM power_cesc_gold.customers
            WHERE cross_brand_opt_in = TRUE

            UNION ALL

            SELECT
                'natures_basket' as brand_code,
                customer_id,
                TO_HEX(SHA256(TO_UTF8(LOWER(TRIM(email))))) as email_hash,
                REGEXP_REPLACE(phone, '[^0-9]', '') as phone_normalized
            FROM natures_basket_gold.customers
            WHERE cross_brand_opt_in = TRUE
        ),
        -- Find cross-brand matches on email_hash
        email_links AS (
            SELECT
                a.brand_code as brand_a,
                a.customer_id as customer_a,
                b.brand_code as brand_b,
                b.customer_id as customer_b,
                'email_hash' as link_type,
                1.0 as confidence
            FROM brand_keys a
            JOIN brand_keys b
                ON a.email_hash = b.email_hash
                AND a.brand_code < b.brand_code
            WHERE a.email_hash IS NOT NULL AND a.email_hash != ''
        ),
        -- Find cross-brand matches on phone
        phone_links AS (
            SELECT
                a.brand_code as brand_a,
                a.customer_id as customer_a,
                b.brand_code as brand_b,
                b.customer_id as customer_b,
                'phone' as link_type,
                0.95 as confidence
            FROM brand_keys a
            JOIN brand_keys b
                ON a.phone_normalized = b.phone_normalized
                AND a.brand_code < b.brand_code
            WHERE a.phone_normalized IS NOT NULL
                AND LENGTH(a.phone_normalized) >= 10
        )
        SELECT * FROM email_links
        UNION ALL
        SELECT * FROM phone_links
        """

        return {
            "status": "linkage_query_compiled",
            "sql": linkage_sql,
            "link_keys": self.CROSS_BRAND_LINK_KEYS,
        }

    @staticmethod
    def hash_email(email: str) -> str:
        """Generate SHA-256 hash of normalized email for privacy-safe matching."""
        normalized = email.strip().lower()
        return hashlib.sha256(normalized.encode()).hexdigest()

    @staticmethod
    def normalize_phone(phone: str) -> str:
        """Normalize phone number: strip non-digits, ensure 10+ digits."""
        digits = "".join(c for c in phone if c.isdigit())
        # If starts with country code 91, strip it
        if len(digits) > 10 and digits.startswith("91"):
            digits = digits[2:]
        return digits
