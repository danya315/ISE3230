import requests
import pandas as pd
import base64
import time
import os
import difflib
import re
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

CLIENT_ID = os.getenv('KROGER_CLIENT_ID')
CLIENT_SECRET = os.getenv('KROGER_CLIENT_SECRET')
MY_ZIP_CODE = os.getenv('MY_ZIP_CODE')
## File Paths
INPUT_CSV = 'grocery_subset.csv' 
OUTPUT_CSV = 'grocery_kroger_priced_2.csv'

## Set to None to process the ENTIRE file (Warning: Can take hours for 100k+ rows)
## Set to an integer (e.g., 5000) to test with a larger batch first.
TARGET_SAMPLE_SIZE = None 


def get_access_token():
    url = "https://api.kroger.com/v1/connect/oauth2/token"
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {b64_auth}"
    }
    data = {"grant_type": "client_credentials", "scope": "product.compact"}
    
    try:
        r = requests.post(url, headers=headers, data=data, timeout=10)
        r.raise_for_status()
        return r.json()['access_token']
    except Exception as e:
        print(f"Auth Failed: {e}")
        return None

def get_location_id(session, token, zip_code):
    url = "https://api.kroger.com/v1/locations"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"filter.zipCode.near": zip_code, "filter.limit": 1}
    try:
        r = session.get(url, headers=headers, params=params, timeout=10)
        if r.json()['data']:
            loc = r.json()['data'][0]
            print(f"📍 Store: {loc['name']} (ID: {loc['locationId']})")
            return loc['locationId']
    except:
        pass
    print("Store not found")
    return None

## Search logic
def search_product(session, brand, name, location_id, token):
    url = "https://api.kroger.com/v1/products"
    headers = {"Authorization": f"Bearer {token}"}
    
    ## Data Cleaning
    clean_name = str(name).replace('"', '').replace(',', '').split(' - ')[0]
    clean_brand = str(brand)
    
    ## Handle Generics
    if clean_brand.lower() in ['nan', 'unknown', 'generic', 'none']:
        search_term = clean_name
    else:
        search_term = f"{clean_brand} {clean_name}"
    
    params = {
        "filter.term": search_term,
        "filter.locationId": location_id,
        "filter.limit": 1 
    }
    
    try:
        r = session.get(url, headers=headers, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json().get('data')
            if data:
                product = data[0]
                found_name = product.get('description', 'Unknown')
                
                # --- MATCH QUALITY SCORE ---
                # Compare what we searched for vs. what Kroger returned.
                # Returns a float between 0.0 (no match) and 1.0 (perfect match).
                match_score = difflib.SequenceMatcher(None, search_term.lower(), found_name.lower()).ratio()
                
                # Extract Price
                items = product.get('items', [{}])[0]
                prices = items.get('price', {})
                price = prices.get('promo', prices.get('regular', 0))
                
                # Filter out garbage matches (e.g. < 20% similarity)
                if price > 0 and match_score > 0.2:
                    return price, found_name, round(match_score * 100, 1)
                    
    except Exception as e:
        pass
        
    return None, None, 0


if __name__ == "__main__":
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found.")
        exit()

    ## Data prep
    print("Loading data...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    
    if TARGET_SAMPLE_SIZE and len(df) > TARGET_SAMPLE_SIZE:
        print(f"⚠️ Sampling {TARGET_SAMPLE_SIZE} random rows for speed...")
        df_processing = df.sample(n=TARGET_SAMPLE_SIZE, random_state=42).copy()
    else:
        print(f"Processing FULL dataset ({len(df)} rows)...")
        df_processing = df.copy()

    ## Setup
    token = get_access_token()
    if not token: exit()
    
    session = requests.Session() 
    location_id = get_location_id(session, token, MY_ZIP_CODE)
    if not location_id: exit()

    print(f"\nStarting search")
    results = []
    
    try:
        # TQDM progress bar
        for index, row in tqdm(df_processing.iterrows(), total=len(df_processing)):
            brand = row['Brand']
            name = row['Product_Name']
            
            price, kroger_name, score = search_product(session, brand, name, location_id, token)
            
            if price:
                row['Price'] = price
                row['Kroger_Name'] = kroger_name
                row['Match_Score'] = score
                row['Price_Source'] = "Kroger API"
                results.append(row)
            
            ## API sleep
            time.sleep(0.06)
            
    except KeyboardInterrupt:
        print("\n Stopping & saving progress...")

    ## Save
    if results:
        final_df = pd.DataFrame(results)
        
        ## Clean columns
        cols = ['UPC', 'Brand', 'Product_Name', 'Price', 'Kroger_Name', 'Match_Score', 'Price_Source']
        ## Add nutrition columns back
        nutrition_cols = [c for c in final_df.columns if c not in cols]
        final_df = final_df[cols + nutrition_cols]
        
        final_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nDone! Found {len(final_df)} prices.")
        print(f"Saved to: {OUTPUT_CSV}")
        print("\nSample Data:")
        print(final_df[['Product_Name', 'Kroger_Name', 'Match_Score', 'Price']].head())
    else:
        print("\nNo prices found.")
