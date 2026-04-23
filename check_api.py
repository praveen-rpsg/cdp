import requests
import json

def verify():
    print("--- E2E VERIFICATION: NEW REPO CONTAINERS ---")
    
    # 1. Test Attribute Catalog
    try:
        r = requests.get('http://localhost:8000/api/v1/segments/attributes/catalog')
        r.raise_for_status()
        data = r.json()
        attrs = data.get('attributes', [])
        bt_attrs = [a for a in attrs if a['key'].startswith('bt.')]
        print(f"✅ Attribute Catalog: Found {len(bt_attrs)} BT attributes (Total: {len(attrs)})")
        if not bt_attrs:
            print("❌ FAIL: BT attributes missing from catalog")
    except Exception as e:
        print(f"❌ FAIL: Could not reach Attribute Catalog API: {e}")

    # 2. Test Audience Estimation (Logic + DB Connection)
    try:
        payload = {
            "brand_code": "spencers",
            "rules": {
                "root": {
                    "logical_operator": "and",
                    "conditions": [
                        {
                            "condition_type": "attribute",
                            "attribute_key": "bt.family_desc",
                            "operator": "equals",
                            "value": "BEVERAGES"
                        }
                    ]
                }
            }
        }
        r = requests.post('http://localhost:8000/api/v1/segments/estimate', json=payload)
        r.raise_for_status()
        res = r.json()
        print(f"✅ Audience Estimation: Received count {res.get('estimated_count')}")
        print(f"✅ Generated SQL Preview: {res.get('sql')[:100]}...")
        if res.get('estimated_count') is None:
            print("❌ FAIL: Audience count is None")
    except Exception as e:
        print(f"❌ FAIL: Audience Estimation failed: {e}")

if __name__ == "__main__":
    verify()
