import sys
import os

# Add the backend to path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.query_engine.pg_compiler import PgCompiler
from app.schemas.segment_rules import SegmentDefinition

def verify():
    print("--- VERIFICATION: NEW REPO MIGRATION SUCCESS ---")
    
    compiler = PgCompiler(brand_code="spencers")
    
    # 1. Test ILIKE for Demographics
    rules_city = {
        "root": {
            "logical_operator": "and",
            "conditions": [
                {
                    "condition_type": "attribute",
                    "attribute_key": "demographic.city",
                    "operator": "equals",
                    "value": "kolkata"
                }
            ]
        }
    }
    def_city = SegmentDefinition.model_validate(rules_city)
    sql_city = compiler.compile(def_city)
    print("\n1. Demographic City (should use p.city and ILIKE):")
    print(sql_city)
    
    # 2. Test EXISTS for BT Attribute
    rules_bt = {
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
    def_bt = SegmentDefinition.model_validate(rules_bt)
    sql_bt = compiler.compile(def_bt)
    print("\n2. BT Attribute (should use EXISTS and bt.family_desc ILIKE):")
    print(sql_bt)
    
    # 3. Validation
    success = True
    if "ILIKE" not in sql_city:
        print("FAIL: ILIKE missing in demographic query")
        success = False
    if "EXISTS" not in sql_bt:
        print("FAIL: EXISTS missing in BT query")
        success = False
    if "p.city" not in sql_city:
        print("FAIL: Incorrect table mapping for city")
        success = False
        
    if success:
        print("\n✅ MIGRATION VERIFIED: Logic is correctly ported.")
    else:
        print("\n❌ MIGRATION FAILED: Check the logic porting.")

if __name__ == "__main__":
    verify()
