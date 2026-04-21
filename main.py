import requests
import pandas as pd
import time

def download_csv(url, csv_suffix):
    page = 1
    all_data = []

    while True:
        params = {
            "page": page,
            "limit": 10000,
            "coo_all": "true",
            "coa_all": "true"
        }

        r = requests.get(url, params=params)
        print("Status:", r.status_code)
        print("Content:", r.text[:200])  # first 200 chars
        data = r.json()

        items = data.get("items", [])

        if not items:
            break

        all_data.extend(items)

        print(f"Strona {page}, rekordów: {len(items)}")

        page += 1

    df = pd.DataFrame(all_data)
    df.to_csv(f"unhcr_{csv_suffix}.csv", index=False)

    print("Zapisano:", len(df), "rekordów")

if __name__=="__main__":
    urlpopulation = "https://api.unhcr.org/population/v1/population/"
    urldemographics = "https://api.unhcr.org/population/v1/demographics/"
    download_csv(urlpopulation, "population")
    download_csv(urldemographics, "demographics")