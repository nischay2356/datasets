


import pandas as pd
import pyarrow.parquet as pq
from datetime import timedelta
import sys
import os

def process_parquet_file(parquet_file_path, output_dir):
    # Read the Parquet file into a PyArrow Table
    table = pq.read_table(parquet_file_path)

    # Convert the PyArrow Table to a Pandas DataFrame
    df = table.to_pandas()

    # Convert timestamp to datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')

    # Create a new column to store trip numbers
    df['trip_number'] = -1

    # Iterate through the DataFrame to identify trips
    trip_number = 0
    for i in range(1, len(df)):
        time_diff = (df['timestamp'].iloc[i] - df['timestamp'].iloc[i - 1]).total_seconds()
        
        if time_diff > 7 * 3600:
            trip_number += 1
        
        df.at[i, 'trip_number'] = trip_number

    # Filter out rows where trip_number is -1 (not part of any trip)
    df = df[df['trip_number'] != -1]

    # Drop the 'unit' column
    df = df.drop(columns=['unit'])

    # Save each trip to a separate CSV file in the specified output directory
    for trip_number, trip_data in df.groupby('trip_number'):
        trip_data.drop(columns=['trip_number'], inplace=True)
        trip_data.to_csv(os.path.join(output_dir, f'trip_{trip_number}.csv'), index=False)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python process1.py <parquet_file_path> <output_dir>")
        sys.exit(1)

    parquet_file_path = sys.argv[1]
    output_dir = sys.argv[2]
    process_parquet_file(parquet_file_path, output_dir)

