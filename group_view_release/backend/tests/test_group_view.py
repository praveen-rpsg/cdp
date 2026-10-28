import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.api.v1 import segments as segments_api
from app.schemas.api_schemas import AudienceEstimateRequest


class FakeService:
    def __init__(self, payload):
        self.payload = payload

    async def estimate_audience_size(self, **kwargs):
        self.payload.update(kwargs)
        return {
            "brand_code": kwargs.get("brand_code"),
            "estimated_count": 77 if kwargs.get("communication_group_view") else 42,
            "sql": "SELECT 1",
            "status": "completed",
            "view_mode": "communication" if kwargs.get("communication_group_view") else None,
        }

    async def get_segment_summary(self, **kwargs):
        self.payload.update(kwargs)
        return {
            "brand_code": kwargs.get("brand_code"),
            "audience_size": 77 if kwargs.get("communication_group_view") else 42,
            "metrics": {"total_spend": 0},
            "sql": "SELECT 2",
            "status": "completed",
        }

    async def preview_audience(self, **kwargs):
        self.payload.update(kwargs)
        return {
            "brand_code": kwargs.get("brand_code"),
            "profiles": [{"id": 1}] if not kwargs.get("communication_group_view") else [],
            "sql": "SELECT 3",
            "status": "completed",
        }


def test_estimate_route_uses_group_view_for_power_cesc():
    payload = {}
    old_service = segments_api.service
    segments_api.service = FakeService(payload)
    try:
        req = AudienceEstimateRequest(
            brand_code="power_cesc",
            rules={"root": {"type": "group", "logical_operator": "and", "conditions": []}},
            communication_group_view=True,
        )
        result = asyncio.run(segments_api.estimate_audience(req))
    finally:
        segments_api.service = old_service

    assert payload["communication_group_view"] is True
    assert result["estimated_count"] == 77
    assert result["view_mode"] == "communication"


def test_estimate_route_keeps_live_audience_unchanged_by_default():
    payload = {}
    old_service = segments_api.service
    segments_api.service = FakeService(payload)
    try:
        req = AudienceEstimateRequest(
            brand_code="spencers",
            rules={"root": {"type": "group", "logical_operator": "and", "conditions": []}},
            communication_group_view=False,
        )
        result = asyncio.run(segments_api.estimate_audience(req))
    finally:
        segments_api.service = old_service

    assert payload["communication_group_view"] is False
    assert result["estimated_count"] == 42
    assert result["view_mode"] is None


def test_summary_and_preview_routes_forward_group_view_flag():
    payload = {}
    old_service = segments_api.service
    segments_api.service = FakeService(payload)
    try:
        summary_req = segments_api.AudienceSummaryRequest(
            brand_code="power_cesc",
            rules={"root": {"type": "group", "logical_operator": "and", "conditions": []}},
            communication_group_view=True,
        )
        summary_result = asyncio.run(segments_api.get_segment_summary(summary_req))

        preview_req = segments_api.AudiencePreviewRequest(
            brand_code="power_cesc",
            rules={"root": {"type": "group", "logical_operator": "and", "conditions": []}},
            communication_group_view=True,
        )
        preview_result = asyncio.run(segments_api.preview_audience(preview_req))
    finally:
        segments_api.service = old_service

    assert payload["communication_group_view"] is True
    assert summary_result["audience_size"] == 77
    assert len(preview_result["profiles"]) == 0
