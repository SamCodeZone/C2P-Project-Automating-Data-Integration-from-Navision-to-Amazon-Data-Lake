import requests
from pathlib import Path
import json

API_BASE_URL = "API URL"
API_KEY = "API KEY"
JOB_ID = "JOB ID" # FROM THE MAIN SCRIPT AFTER RUN
cert_path = r"CERTIFICATE PATH"

# Send the ingestion request and save the response        
response = requests.get(
    url=f"{API_BASE_URL}/jobs/{JOB_ID}/status",
    #url=f"{API_BASE_URL}/flows/schema", 
    headers={"x-api-key": API_KEY},
    verify=cert_path 
)
body = response.json()
print(body)
