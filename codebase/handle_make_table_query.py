import os
import csv

def handle_make_table_query(table_name, columns, types):
    base_directory = "table_chunks"
    table_directory = os.path.join(base_directory, table_name)
    os.makedirs(table_directory, exist_ok=True)


    csv_file_path = os.path.join(table_directory, f"{table_name}_chunk1.csv")
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()

    return f"Table '{table_name}' created successfully with columns: {', '.join(columns)}."
