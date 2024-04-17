import re
import os
import csv
import ast
from prettytable import PrettyTable

def handle_enter_query(table_name, values_str, max_chunk_size=25 * 1024 * 1024):  # Max chunk size is 50 MB
    rows = ast.literal_eval(values_str)

    base_directory = "table_chunks"
    table_directory = os.path.join(base_directory, table_name)

    if not os.path.exists(table_directory):
        os.makedirs(table_directory, exist_ok=True)

    chunk_number = 1
    while True:
        csv_file_path = os.path.join(table_directory, f"{table_name}_chunk{chunk_number}.csv")
        if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) < max_chunk_size:
            break
        chunk_number += 1

    with open(csv_file_path, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for row in rows:
            writer.writerow(row)

    return f"Data entered successfully into table '{table_name}' in chunk {chunk_number}."