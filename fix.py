import os
import pyarrow.parquet as pq

directory = "./D"

for filename in os.listdir(directory):
    if filename.endswith(".parquet"):
        file_path = os.path.join(directory, filename)

        table = pq.read_table(file_path)

        column_names = table.column_names

        filtered_column_names = [col for col in column_names if "Unnamed" not in col]

        filtered_table = table.select(filtered_column_names)

        output_path = os.path.join(directory, filename)
        pq.write_table(filtered_table, output_path)

        print(f"Processed file: {filename}")
