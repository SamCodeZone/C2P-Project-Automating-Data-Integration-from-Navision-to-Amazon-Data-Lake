import requests
from pathlib import Path

url = "YOUR URL"
api_key = "API_KEY"
cert_path = r"CERTIFICATE_PATH" #THIS CERTIFICATE IS DOWNLOAD LOCALLY ON THE MACHINE
headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "Identity",
        "X-API-Key": api_key
}

FLOW_ID = "YOUR FLOW ID" 

flow_companion=Path('JSONNAME.json').read_text()

x=requests.put(
    url=f"{url}/flows/{FLOW_ID}", 
    headers={"x-api-key": api_key}, 
    data=flow_companion,
    verify=cert_path
)
print(x.json())
