import os
import glob
import pandas as pd

def clean_viirs_fires(input_folder, output_folder):
    print(f"Checking input directory: {input_folder}")
    os.makedirs(output_folder, exist_ok=True)
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    print(f"Found {len(csv_files)} CSV files.")
    
    if not csv_files:
        print("No CSV files found.")
        return

    yearly_data = {}

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print(f"Processing file: {file_name}")
        try:
            df = pd.read_csv(file_path)
            print(f"Loaded {file_name} with {len(df)} rows.")
            
            required_cols = ["acq_date", "confidence", "latitude", "longitude", "type"]
            if not all(col in df.columns for col in required_cols):
                print(f"Skipping {file_name}: Missing required columns.")
                continue

            df["type"] = pd.to_numeric(df["type"], errors="coerce")
            df = df[df["type"] == 0]
            print(f"After type 0 filter: {len(df)} rows remain.")
            if df.empty:
                print(f"Skipping {file_name}: No type 0 rows found.")
                continue

            df["acq_date"] = pd.to_datetime(df["acq_date"], errors="coerce")
            df["year"] = df["acq_date"].dt.year

            df["confidence"] = df["confidence"].astype(str).str.lower().str.strip()
            df = df[df["confidence"].isin(["n", "h"])]
            print(f"After confidence filter ('n', 'h'): {len(df)} rows remain.")
            if df.empty:
                print(f"Skipping {file_name}: No rows passed confidence filter.")
                continue

            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
            df = df.dropna(subset=["latitude", "longitude", "year", "acq_date", "confidence"])
            df = df[(df["latitude"] >= -90) & (df["latitude"] <= 90) & (df["longitude"] >= -180) & (df["longitude"] <= 180)]
            df["year"] = df["year"].astype(int)
            print(f"After coordinate validation: {len(df)} rows remain.")

            for year, group in df.groupby("year"):
                if year not in yearly_data:
                    yearly_data[year] = []
                yearly_data[year].append(group)
                print(f"Added {len(group)} rows to year {year}.")

        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            continue

    print("Saving yearly aggregated files...")
    for year, dfs in yearly_data.items():
        combined_year_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
        output_file = os.path.join(output_folder, f"viirs_wildfires_{year}.csv")
        combined_year_df.to_csv(output_file, index=False)
        print(f"Saved {len(combined_year_df)} rows to {output_file}")

if __name__ == "__main__":
    INPUT_DIRECTORY = ""
    OUTPUT_DIRECTORY = "output"
    clean_viirs_fires(INPUT_DIRECTORY, OUTPUT_DIRECTORY)