import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

def get_aaa_florida_expanded():
    """Scrapes AAA for National, Florida, and specific SWFL regional prices."""
    url = "https://gasprices.aaa.com/?state=FL"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    
    now = datetime.now()
    result = {
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "national_regular": None,
        "fl_regular": None,
        "fl_premium": None,
        "naples_regular": None,
        "naples_premium": None,
        "punta_gorda_regular": None,
        "punta_gorda_premium": None,
        "bradenton_sarasota_regular": None,
        "bradenton_sarasota_premium": None
    }
    
    try:
        print("Fetching Expanded AAA Florida prices...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. Get National Average (from the red circular badge)
        for div in soup.find_all('div', class_='average-price'):
            if 'average-price--blue' not in div.get('class', []):
                numb = div.find('p', class_='numb')
                if numb:
                    result['national_regular'] = numb.text.strip().split()[0].replace('$', '')
                    break

        # 2. Get Florida State Average (from the primary state table)
        main_table = soup.find('table', class_='table-mob')
        if main_table:
            current_row = main_table.find('tbody').find_all('tr')[0]
            cells = current_row.find_all('td')
            if len(cells) >= 4 and "Current Avg." in cells[0].text:
                result["fl_regular"] = cells[1].text.strip().replace('$', '')
                result["fl_premium"] = cells[3].text.strip().replace('$', '')

        # 3. Helper function to extract specific Metro areas
        def extract_metro_price(metro_keyword):
            for h3 in soup.find_all('h3'):
                if metro_keyword.lower() in h3.text.lower():
                    # The table is stored in the div immediately following the h3 header
                    content_div = h3.find_next_sibling('div')
                    if content_div:
                        table = content_div.find('table', class_='table-mob')
                        if table:
                            row = table.find('tbody').find_all('tr')[0]
                            cells = row.find_all('td')
                            if len(cells) >= 4 and "Current Avg." in cells[0].text:
                                reg = cells[1].text.strip().replace('$', '')
                                prem = cells[3].text.strip().replace('$', '')
                                return reg, prem
            return None, None

        # Extract our target regions
        result["naples_regular"], result["naples_premium"] = extract_metro_price("Naples")
        result["punta_gorda_regular"], result["punta_gorda_premium"] = extract_metro_price("Punta Gorda")
        result["bradenton_sarasota_regular"], result["bradenton_sarasota_premium"] = extract_metro_price("Bradenton-Sarasota-Venice")

        return result

    except Exception as e:
        print(f"Error scraping AAA: {e}")
        return result

def save_aaa_to_csv(data, filename="aaa_prices.csv"):
    df_new = pd.DataFrame([data])
    
    if os.path.isfile(filename):
        df_existing = pd.read_csv(filename)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        # Drop exact duplicates using all columns to prevent spamming the CSV on multiple runs
        df_combined.drop_duplicates(subset=list(data.keys()), keep='last', inplace=True)
    else:
        df_combined = df_new
        
    df_combined.to_csv(filename, index=False)
    print(f"Successfully aggregated data and removed duplicates in {filename}")

if __name__ == "__main__":
    aaa_data = get_aaa_florida_expanded()
    
    for key, value in aaa_data.items():
        print(f"{key}: {value}")
        
    if aaa_data["fl_regular"]:
        save_aaa_to_csv(aaa_data)
    else:
        print("Failed to locate Florida prices. CSV export skipped.")