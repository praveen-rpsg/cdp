import asyncio
import json
from app.services.segmentation.service import SegmentationService
from app.schemas.api_schemas import AudienceSummaryRequest

async def verify_summary():
    service = SegmentationService()
    
    # 1. Define a broad segment for verification (e.g., Spencers brand)
    # This segment has no rules, so it should include everyone in that brand
    brand_code = "spencers"
    rules = {
        "root": {
            "type": "group",
            "logical_operator": "and",
            "conditions": []
        }
    }
    
    print(f"--- Verifying Summary Statistics for Brand: {brand_code} ---")
    
    # 2. Get the summary
    try:
        # We call the service method directly
        # Note: In a real environment, you might need to ensure the DB is reachable
        result = await service.get_segment_summary(
            brand_code=brand_code,
            rules=rules
        )
        
        print("\n[STEP 1] Generated SQL for Summary Statistics:")
        print("-" * 50)
        print(result.sql)
        print("-" * 50)
        
        print("\n[STEP 2] Calculated Metrics:")
        print(json.dumps(result.metrics, indent=4))
        
        print(f"\n[STEP 3] Total Audience Size: {result.audience_size}")
        
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Ensure we are in the correct directory to import app
    import sys
    import os
    sys.path.append(os.path.join(os.getcwd(), 'backend'))
    
    asyncio.run(verify_summary())
