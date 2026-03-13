import pandas as pd
import numpy as np
import os

def load_and_join_data():
    print("Loading datasets...")
    # 1. Market Data
    try:
        market_df = pd.read_csv("market_prices.csv")
        market_df['Date'] = pd.to_datetime(market_df['Date'])
    except FileNotFoundError:
        print("Error: market_prices.csv not found.")
        return None

    # 2. Costco Data
    try:
        costco_df = pd.read_csv("costco_prices.csv")
        costco_df['Date'] = pd.to_datetime(costco_df['date'])
        costco_df['costco_regular'] = costco_df['regular_price'].astype(str).replace(r'[\$,]', '', regex=True).astype(float)
        costco_df['costco_premium'] = costco_df['premium_price'].astype(str).replace(r'[\$,]', '', regex=True).astype(float)
        costco_df = costco_df[['Date', 'costco_regular', 'costco_premium']]
    except FileNotFoundError:
        costco_df = pd.DataFrame(columns=['Date', 'costco_regular', 'costco_premium'])

    # 3. AAA Regional Data
    try:
        aaa_df = pd.read_csv("aaa_prices.csv")
        aaa_df['Date'] = pd.to_datetime(aaa_df['date'])
        
        aaa_cols_to_clean = [
            'fl_regular', 'fl_premium', 'naples_regular', 'naples_premium',
            'punta_gorda_regular', 'punta_gorda_premium', 
            'bradenton_sarasota_regular', 'bradenton_sarasota_premium'
        ]
        
        for col in aaa_cols_to_clean:
            if col in aaa_df.columns:
                aaa_df[col] = aaa_df[col].astype(str).replace(r'[\$,]', '', regex=True).astype(float)
                
        cols_to_keep = ['Date'] + [c for c in aaa_cols_to_clean if c in aaa_df.columns]
        aaa_df = aaa_df[cols_to_keep]
    except FileNotFoundError:
        aaa_df = pd.DataFrame(columns=['Date'])

    # 4. News Sentiment Data
    try:
        news_df = pd.read_csv("news_sentiment.csv")
        news_df['Date'] = pd.to_datetime(news_df['date_scraped'])
        news_df['weighted_score'] = news_df['signal'] * news_df['confidence_score']
        daily_sentiment = news_df.groupby('Date')['weighted_score'].sum().reset_index()
        daily_sentiment.rename(columns={'weighted_score': 'net_news_sentiment'}, inplace=True)
    except FileNotFoundError:
         daily_sentiment = pd.DataFrame(columns=['Date', 'net_news_sentiment'])

    # --- THE BIG JOIN ---
    print("Joining data on Date index...")
    master_df = pd.merge(market_df, costco_df, on='Date', how='left')
    master_df = pd.merge(master_df, aaa_df, on='Date', how='left')
    master_df = pd.merge(master_df, daily_sentiment, on='Date', how='left')

    # Forward fill missing days FIRST (for weekends)
    cols_to_fill = [c for c in master_df.columns if c not in ['Date', 'net_news_sentiment']]
    master_df[cols_to_fill] = master_df[cols_to_fill].ffill()
    
    # THE FIX: Synthetic Historical Backfill
    # If we have RBOB futures, we can estimate historical retail prices
    # Retail ~= RBOB + $0.53 (Taxes) + $0.40 (Retail Markup)
    synthetic_regular = master_df['RBOB_Gasoline'] + 0.93
    synthetic_premium = master_df['RBOB_Gasoline'] + 1.65 # Premium usually 70 cents higher
    
    # Fill any remaining NaNs (the past year) with our synthetic baseline
    if 'costco_regular' in master_df.columns:
        master_df['costco_regular'] = master_df['costco_regular'].fillna(synthetic_regular - 0.20) # Costco is ~20c cheaper
    if 'fl_regular' in master_df.columns:
        master_df['fl_regular'] = master_df['fl_regular'].fillna(synthetic_regular)
    if 'naples_regular' in master_df.columns:
        master_df['naples_regular'] = master_df['naples_regular'].fillna(synthetic_regular + 0.10) # Naples is usually pricier
    if 'punta_gorda_regular' in master_df.columns:
        master_df['punta_gorda_regular'] = master_df['punta_gorda_regular'].fillna(synthetic_regular + 0.05)
        
    master_df['net_news_sentiment'] = master_df.get('net_news_sentiment', pd.Series(0, index=master_df.index)).fillna(0)
    
    return master_df

def compute_features(df):
    print("Computing quantitative features...")
    cols = df.columns
    
    # --- 1. RETAIL SPREADS ---
    if 'costco_regular' in cols and 'fl_regular' in cols:
        df['spread_costco_vs_fl'] = df['costco_regular'] - df['fl_regular']
    if 'costco_regular' in cols and 'naples_regular' in cols:
        df['spread_costco_vs_naples'] = df['costco_regular'] - df['naples_regular']
    if 'costco_regular' in cols and 'punta_gorda_regular' in cols:
         df['spread_costco_vs_pg'] = df['costco_regular'] - df['punta_gorda_regular']

    # --- 2. MACRO SPREADS ---
    if 'RBOB_Gasoline' in cols and 'WTI_Crude' in cols:
        df['crack_spread'] = (df['RBOB_Gasoline'] * 42) - df['WTI_Crude']

    # --- 3. MOMENTUM & AUTOCORRELATION ---
    if 'WTI_Crude' in cols:
        df['wti_mom_60d'] = df['WTI_Crude'].pct_change(periods=60)
        df['wti_mom_10d'] = df['WTI_Crude'].pct_change(periods=10)
        
    # --- 4. VOLATILITY ---
    if 'WTI_Crude' in cols:
        daily_returns = df['WTI_Crude'].pct_change()
        df['wti_vol_20d'] = daily_returns.rolling(window=20).std()

    # --- 5. SEASONALITY ---
    df['day_of_week'] = df['Date'].dt.dayofweek
    df['is_weekend_prep'] = np.where(df['day_of_week'].isin([3, 4]), 1, 0)
    
    return df

def assign_market_regime(df):
    print("Assigning market regimes...")
    lookback_window = 126 

    if 'wti_vol_20d' in df.columns:
        df['vol_z_score'] = (df['wti_vol_20d'] - df['wti_vol_20d'].rolling(lookback_window).mean()) / df['wti_vol_20d'].rolling(lookback_window).std()

    if 'wti_mom_60d' in df.columns:
        df['mom_z_score'] = (df['wti_mom_60d'] - df['wti_mom_60d'].rolling(lookback_window).mean()) / df['wti_mom_60d'].rolling(lookback_window).std()

    df['market_regime'] = 'Unknown'
    valid_rows = df.get('vol_z_score', pd.Series()).notna() & df.get('mom_z_score', pd.Series()).notna()

    df.loc[valid_rows & (df['mom_z_score'] > 0) & (df['vol_z_score'] > 0), 'market_regime'] = 'Bull_Volatile'
    df.loc[valid_rows & (df['mom_z_score'] > 0) & (df['vol_z_score'] <= 0), 'market_regime'] = 'Bull_Quiet'
    df.loc[valid_rows & (df['mom_z_score'] <= 0) & (df['vol_z_score'] > 0), 'market_regime'] = 'Bear_Volatile'
    df.loc[valid_rows & (df['mom_z_score'] <= 0) & (df['vol_z_score'] <= 0), 'market_regime'] = 'Bear_Quiet'
    df.loc[valid_rows & (df['vol_z_score'] > 2.5), 'market_regime'] = 'Extreme_Shock'

    # Finally, drop the initial rows where lookbacks caused NaNs, so the model gets clean data
    df.dropna(subset=['wti_mom_60d', 'vol_z_score'], inplace=True)

    return df

if __name__ == "__main__":
    raw_df = load_and_join_data()
    
    if raw_df is not None:
        featured_df = compute_features(raw_df)
        final_df = assign_market_regime(featured_df)
        
        final_df.to_csv("master_features.csv", index=False)
        print("\nSUCCESS: master_features.csv created. Rows without NaNs:", len(final_df))