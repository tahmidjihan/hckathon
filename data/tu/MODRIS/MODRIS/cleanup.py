import glob
import os
import pandas as pd


def clean_and_organize_fires(input_folder, output_folder):
  os.makedirs(output_folder, exist_ok=True)

  csv_files = glob.glob(os.path.join(input_folder, "*.csv"))

  if not csv_files:
    print(f"No CSV files found in '{input_folder}'. Check your path.")
    return

  print(
      f"Found {len(csv_files)} files to process. Starting wildfire cleanup..."
  )

  yearly_data = {}

  for file_path in csv_files:
    file_name = os.path.basename(file_path)
    print(f"Processing: {file_name}")

    try:
      df = pd.read_csv(file_path)

      required_cols = [
          "acq_date",
          "confidence",
          "latitude",
          "longitude",
          "type",
      ]
      if not all(col in df.columns for col in required_cols):
        print(
            f"Skipping {file_name}: Missing one or more required columns."
        )
        continue

      df["type"] = pd.to_numeric(df["type"], errors="coerce")
      df = df[df["type"] == 0]

      if df.empty:
        print(f"No type 0 fires found in {file_name}. Skipping.")
        continue

      df["acq_date"] = pd.to_datetime(df["acq_date"], errors="coerce")
      df["year"] = df["acq_date"].dt.year

      df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
      df = df.dropna(
          subset=["confidence", "year", "latitude", "longitude", "type"]
      )
      df = df[df["confidence"] >= 30]

      df = df[
          (df["latitude"] >= -90)
          & (df["latitude"] <= 90)
          & (df["longitude"] >= -180)
          & (df["longitude"] <= 180)
      ]

      df["year"] = df["year"].astype(int)

      for year, group in df.groupby("year"):
        if year not in yearly_data:
          yearly_data[year] = []
        yearly_data[year].append(group)

    except Exception as e:
      print(f"Error processing {file_name}: {e}")

  for year, dfs in yearly_data.items():
    combined_year_df = pd.concat(dfs, ignore_index=True)

    combined_year_df = combined_year_df.drop_duplicates()

    output_file = os.path.join(output_folder, f"wildfires_{year}.csv")

    combined_year_df.to_csv(output_file, index=False)
    print(
        f"Saved {len(combined_year_df):,} wildfire records for year {year} ->"
        f" {output_file}"
    )

  print("\nAll files successfully filtered for wildfires and organized!")


if __name__ == "__main__":
  INPUT_DIRECTORY = ""
  OUTPUT_DIRECTORY = "output"

  clean_and_organize_fires(INPUT_DIRECTORY, OUTPUT_DIRECTORY)