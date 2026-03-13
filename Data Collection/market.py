import yfinance as yf
import pandas as pd
import datetime

# Expanded tickers specifically for predicting retail gas price movements
TICKER_MAP = {

    "WTI_Crude": "CL=F",
    "Brent_Crude": "BZ=F",
    "RBOB_Gasoline": "RB=F",      # Wholesale gasoline futures
    "Costco_Stock": "COST",       # Costco wholesale stock
    "US_Dollar_Index": "DX-Y.NYB",# Currency strength impacts oil
    "Energy_ETF": "XLE" ,          # Broad energy sector market health

    "Exxon": "XOM",
    "Chvron": "CVX",
    "Marathon_Petro": "MPC",       
    "Phillips_66": "PSX",        
    "Cheniere_LNG": "LNG",
    # The Defense Proxy Basket
    "Lockheed_Martin": "LMT",
    "RTX_Corp": "RTX",
    "Northrop_Grumman": "NOC",
    "L3Harris": "LHX",
    "General_Dynamics": "GD"
    
}

def get_historical_market_data():
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=365)
    
    market_data = pd.DataFrame()
    
    print("Downloading 1-year historical data for predictive modeling...")
    
    for name, ticker in TICKER_MAP.items():
        try:
            # Download the ticker data
            data = yf.download(ticker, start=start_date, end=end_date)
            if not data.empty:
                # Extract just the 'Close' prices and add to our dataframe
                market_data[name] = data['Close'].squeeze()
                print(f"Successfully downloaded {name}")
        except Exception as e:
            print(f"Failed to download {name} ({ticker}): {e}")
            
    # Reset index so 'Date' becomes a standard column instead of an index
    market_data.reset_index(inplace=True)
    
    # Format the Date column nicely
    if 'Date' in market_data.columns:
        market_data['Date'] = pd.to_datetime(market_data['Date']).dt.strftime('%Y-%m-%d')
    
    return market_data

if __name__ == "__main__":
    df = get_historical_market_data()
    df.to_csv("market_prices.csv", index=False)
    print("\nSUCCESS: Historical market data saved to market_prices.csv")