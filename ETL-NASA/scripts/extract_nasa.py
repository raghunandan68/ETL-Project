import json
from pathlib import Path
from datetime import datetime
import requests
DATA_DIR=Path(__file__).resolve().parents[1]/"data"/"raw"
DATA_DIR.mkdir(parents=True,exist_ok=True)
def extract_nasa_data():
    api_key="LIgn3LgCHJhOskLgb2gWxJHePcmoan2yR97zcfR6"
    url="https://api.nasa.gov/planetary/apod?api_key=LIgn3LgCHJhOskLgb2gWxJHePcmoan2yR97zcfR6"
    params={
        "api_key":api_key
    }
    resp=requests.get(url,params=params)
    resp.raise_for_status()
    data=resp.json()
    filename=DATA_DIR/f"NasaData.json"
    filename.write_text(json.dumps(data,indent=2))
    print(f"Extracted nasa data saved to : {filename}")
    return data
if __name__ == "__main__":
    extract_nasa_data()