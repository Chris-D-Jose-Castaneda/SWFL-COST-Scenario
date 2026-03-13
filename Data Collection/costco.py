import webbrowser
from datetime import datetime
import pandas as pd
import os

def save_costco_to_csv(data, filename="costco_prices.csv"):
    # If the user just hit Enter and provided no prices, skip saving
    if not data["regular_price"] and not data["premium_price"]:
        print("No prices entered. Skipping CSV export.")
        return

    df_new = pd.DataFrame([data])
    
    if os.path.isfile(filename):
        df_existing = pd.read_csv(filename)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        # Drop exact duplicates just in case you run it twice
        df_combined.drop_duplicates(subset=['date', 'regular_price', 'premium_price'], keep='last', inplace=True)
    else:
        df_combined = df_new
        
    df_combined.to_csv(filename, index=False)
    print(f"\nSUCCESS: Logged Regular: ${data['regular_price']} | Premium: ${data['premium_price']} to {filename}")

if __name__ == "__main__":
    url = "https://www.costco.com/warehouse-locations/-621.html"
    station = "Costco Fort Myers 351"

    print(f"Opening {station} in your default browser...")
    # This pops open your real Chrome/Firefox browser perfectly
    webbrowser.open(url)
    
    print("\n--- Manual Price Entry ---")
    print("Check the browser, scroll down, and enter the current prices.")
    print("(Just hit Enter if you want to skip)\n")
    
    # Take user input right in the terminal
    regular_input = input("Enter Regular Price (e.g., 3.399): ").strip()
    premium_input = input("Enter Premium Price (e.g., 3.899): ").strip()
    
    # Clean up inputs in case you accidentally type the dollar sign
    regular_clean = regular_input.replace('$', '') if regular_input else None
    premium_clean = premium_input.replace('$', '') if premium_input else None
    
    now = datetime.now()
    scraped_data = {
        "station_name": station,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "regular_price": regular_clean,
        "premium_price": premium_clean
    }
    
    save_costco_to_csv(scraped_data)