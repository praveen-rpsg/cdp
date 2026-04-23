import requests
import json

def test_catalog():
    try:
        r = requests.get("http://localhost:8000/api/v1/segments/attributes/catalog")
        print(f"Status Code: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            print(f"Total attributes: {data['total']}")
            print(f"First attribute label: {data['attributes'][0]['label'] if data['attributes'] else 'NONE'}")
            print(f"Categories: {data['categories']}")
        else:
            print(f"Error: {r.text}")
    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    test_catalog()
