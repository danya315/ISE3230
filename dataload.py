import pandas as pd
import requests
import os
import sys

## Configuration
FILE_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
INPUT_FILE = 'en.openfoodfacts.org.products.csv.gz'
OUTPUT_FILE = 'nutrition_data_full.csv'

## Only keep items sold in the US (to ensure UPCs are relevant)
TARGET_COUNTRY = "united states"

## Columns to keep (UPC, Name, Brand, Nutrients)
cols_to_use = [
    'code', 'product_name', 'brands', 'countries_en',
    'energy-kcal_100g', 'proteins_100g', 'carbohydrates_100g', 
    'fat_100g', 'sodium_100g'
]

## Download file
def download_file(url, filename):
    print(f"Downloading database from {url}...")
    print("This is a large file (approx 500MB - 5GB). Please wait...")
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_length = r.headers.get('content-length')
        
        with open(filename, 'wb') as f:
            if total_length is None: # No content length header
                f.write(r.content)
            else:
                dl = 0
                total_length = int(total_length)
                for data in r.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                    sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {int(dl/total_length*100)}%")
                    sys.stdout.flush()
    print("\nDownload complete!")

## Check if file exists. If not, try to find it in Downloads or Download it.
if not os.path.exists(INPUT_FILE):
    # Check user's Downloads folder just in case
    downloads_path = os.path.expanduser(f"~/Downloads/{INPUT_FILE}")
    if os.path.exists(downloads_path):
        print(f"Found file in Downloads: {downloads_path}")
        INPUT_FILE = downloads_path
    else:
        print(f"File not found locally. Starting auto-download...")
        try:
            download_file(FILE_URL, INPUT_FILE)
        except Exception as e:
            print(f"Error downloading: {e}")
            exit()

## Process the file
print(f"\nProcessing {INPUT_FILE}...")
chunk_size = 100000 
clean_rows = []

try:
    with pd.read_csv(
        INPUT_FILE, 
        sep='\t', 
        compression='gzip', 
        usecols=cols_to_use, 
        chunksize=chunk_size, 
        low_memory=False,
        on_bad_lines='skip'
    ) as reader:
        
        for i, chunk in enumerate(reader):
            ## Filter for US products
            chunk = chunk.dropna(subset=['countries_en'])
            us_items = chunk[chunk['countries_en'].str.lower().str.contains(TARGET_COUNTRY, na=False)]
            
            ## Filter for valid nutrition
            valid_items = us_items.dropna(subset=['energy-kcal_100g'])
            
            ## Rename columns
            valid_items = valid_items.rename(columns={
                'code': 'UPC',
                'product_name': 'Product_Name',
                'brands': 'Brand',
                'energy-kcal_100g': 'Calories',
                'proteins_100g': 'Protein_g',
                'carbohydrates_100g': 'Carbs_g',
                'fat_100g': 'Fat_g',
                'sodium_100g': 'Sodium_g'
            })
            
            if not valid_items.empty:
                clean_rows.append(valid_items)
            
            ## Progress indicator (every 10 chunks)
            if i % 10 == 0:
                print(f"Processed chunk {i}... Found {sum(len(c) for c in clean_rows)} US items so far.")

    ## Save Final File
    if clean_rows:
        print("Combining data...")
        final_df = pd.concat(clean_rows, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['UPC'])
        
        ## Select final columns
        final_cols = ['UPC', 'Brand', 'Product_Name', 'Calories', 'Protein_g', 'Carbs_g', 'Fat_g', 'Sodium_g']
        final_df = final_df[final_cols]
        
        print(f"Saving {len(final_df)} items to {OUTPUT_FILE}...")
        final_df.to_csv(OUTPUT_FILE, index=False)
        print("SUCCESS! You can now use this file to add prices.")
    else:
        print("No US items found. Check the input file.")

except Exception as e:
    print(f"An error occurred: {e}")
