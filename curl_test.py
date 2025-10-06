#!/usr/bin/env python3
import requests
import json
import time

# Wait for server to start
time.sleep(2)

url = "http://localhost:5001/api/scraper/facebook/run"
headers = {"Content-Type": "application/json"}

# Load payload from file
with open("facebook_test_payload.json", "r") as f:
    payload = json.load(f)

print("Testing Facebook scraper endpoint...")
print(f"URL: {url}")
print(f"Payload: {json.dumps(payload, indent=2)}")
print()

try:
    response = requests.post(url, json=payload, headers=headers, timeout=300)  # 5 minute timeout
    print(f"Status Code: {response.status_code}")
    print("Response:")
    print(json.dumps(response.json(), indent=2))
except requests.exceptions.RequestException as e:
    print(f"❌ Request failed: {e}")
except json.JSONDecodeError:
    print("Response (not JSON):")
    print(response.text)