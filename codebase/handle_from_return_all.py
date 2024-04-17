import os
import csv
from prettytable import PrettyTable
import ast 

def handle_from_return_all(table_name, return_columns=None, row_limit=None, has_headers=True):
    base_directory = "table_chunks"
    table_directory = os.path.join(base_directory, table_name)

    if not os.path.exists(table_directory):
        return f"Table '{table_name}' does not exist."

    table = PrettyTable()
    table.align = "l"
    row_count = 0

    for chunk_file in sorted(os.listdir(table_directory)):
        if chunk_file.endswith(".csv"):
            csv_file_path = os.path.join(table_directory, chunk_file)
            with open(csv_file_path, 'r') as csv_file:
                reader = csv.reader(csv_file)
                headers = next(reader) if has_headers else [f"Column{i+1}" for i in range(len(next(reader)))]

                if return_columns and return_columns != "ALL":
                    try:
                        column_indices = [headers.index(col.strip()) for col in ast.literal_eval(return_columns)]
                    except ValueError:
                        return f"Invalid return columns: {return_columns}"
                    table.field_names = [headers[i] for i in column_indices]
                else:
                    table.field_names = headers

                for row in reader:
                    if row_limit is not None and row_count >= row_limit:
                        break
                    table.add_row([row[i] for i in column_indices] if return_columns and return_columns != "ALL" else row)
                    row_count += 1

    print(table)
    return "Data displayed successfully."
