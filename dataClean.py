import pandas as pd
import os

## Configuration
INPUT_CSV = 'nutrition_data_full.csv'
OUTPUT_CLEAN_CSV = 'nutrition_data_clean.csv'
OUTPUT_AI_CSV = 'grocery_ai_ready.csv' ## The final small file for AI price tagging

TARGET_ROW_COUNT = 1500 

STAPLE_KEYWORDS = [
    'milk', 'eggs', 'butter', 'yogurt', 'cheese', 'cream',
    'bread', 'bagel', 'tortilla', 'rice', 'pasta', 'noodle', 'quinoa',
    'chicken', 'beef', 'pork', 'turkey', 'bacon', 'sausage', 'fish', 'tuna',
    'apple', 'banana', 'grape', 'berry', 'potato', 'onion', 'carrot', 'tomato',
    'corn', 'bean', 'pea', 'soup', 'broth', 'sauce', 'ketchup', 'mustard',
    'cereal', 'oat', 'granola', 'pancake', 'waffle',
    'chip', 'cracker', 'cookie', 'chocolate', 'candy', 'popcorn',
    'soda', 'juice', 'coffee', 'tea', 'water',
    'pizza', 'frozen', 'ice cream'
]

## Clean and optimize
if os.path.exists(INPUT_CSV):
    print(f"Loading {INPUT_CSV}...")
    
    ## Load the data
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    initial_count = len(df)
    
    print(f"Original Row Count: {initial_count}")
    
    ## Drop rows with missing data
    df = df.dropna(how='any')
    
    ## Drop rows with all 0s
    df = df[
        (df['Calories'] > 0) | 
        (df['Protein_g'] > 0) | 
        (df['Carbs_g'] > 0)
    ]
    
    final_count = len(df)
    dropped_count = initial_count - final_count
    print(f"Rows Dropped: {dropped_count}")
    print(f"Clean Row Count: {final_count}")
    
    ## Reduce size of dataset for optimization
    
    ## Round to one decimal place
    cols_to_round = ['Calories', 'Protein_g', 'Carbs_g', 'Fat_g', 'Sodium_g']
    # Ensure columns exist before rounding
    existing_cols = [c for c in cols_to_round if c in df.columns]
    df[existing_cols] = df[existing_cols].round(1)

    ## Delete Duplicates
    df = df.sort_values('UPC')
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['Brand', 'Product_Name'], keep='first')
    print(f"Removed {before_dedup - len(df)} duplicate product sizes.")

    ## Delete uncommon brands (i.e., keep only common brands that can be found elsewhere)
    brand_counts = df['Brand'].value_counts()
    major_brands = brand_counts[brand_counts >= 10].index
    df = df[df['Brand'].isin(major_brands)]
    print(f"Removed single-product brands. Current count: {len(df)}")

    ## Save the Intermediate Clean File
    print(f"Saving clean data to {OUTPUT_CLEAN_CSV}...")
    df.to_csv(OUTPUT_CLEAN_CSV, index=False)
    print("Clean save complete.")

    ## Prepare nutrition data to be tagged by AI
    print("\n--- Starting AI Curation ---")
    
    pattern = '|'.join(STAPLE_KEYWORDS)
    df_staples = df[df['Product_Name'].str.contains(pattern, case=False, na=False)].copy()
    
    print(f"Found {len(df_staples)} staple items matching keywords.")

    ## Categorize items to ensure variety
    def assign_category(name):
        name = name.lower()
        if any(x in name for x in ['milk', 'yogurt', 'cheese', 'butter', 'cream']): return 'Dairy'
        if any(x in name for x in ['beef', 'chicken', 'pork', 'fish', 'turkey', 'bacon']): return 'Meat'
        if any(x in name for x in ['apple', 'banana', 'vegetable', 'carrot', 'potato', 'onion']): return 'Produce'
        if any(x in name for x in ['bread', 'bagel', 'pasta', 'rice', 'noodle']): return 'Grains'
        if any(x in name for x in ['soda', 'juice', 'coffee', 'water', 'tea']): return 'Beverages'
        if any(x in name for x in ['chip', 'cookie', 'candy', 'cracker']): return 'Snacks'
        if any(x in name for x in ['pizza', 'ice cream', 'frozen']): return 'Frozen'
        return 'Pantry'

    df_staples['Category'] = df_staples['Product_Name'].apply(assign_category)

    ## Limit to Target Count
    categories = df_staples['Category'].unique()
    
    if len(categories) > 0:
        items_per_cat = TARGET_ROW_COUNT // len(categories)
        
        final_dfs = []
        for cat in categories:
            cat_df = df_staples[df_staples['Category'] == cat]
            ## Take the top N items per category
            final_dfs.append(cat_df.head(items_per_cat))
            
        if final_dfs:
            df_final = pd.concat(final_dfs).sample(frac=1).reset_index(drop=True) # Shuffle
            
            ## Save Final AI File
            print(f"Saving {len(df_final)} AI-ready rows to {OUTPUT_AI_CSV}...")
            df_final.to_csv(OUTPUT_AI_CSV, index=False)
            print("Success! AI Dataset is ready.")
        else:
            print("Error: No data remained after categorization.")
    else:
        print("Error: No categories found.")

else:
    print(f"Error: Could not find '{INPUT_CSV}'.")
