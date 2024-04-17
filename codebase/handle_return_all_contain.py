import os
import csv
from prettytable import PrettyTable
import ast  

def handle_return_all_contain(table_name, return_columns=None, row_limit=None, has_headers=True):
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

                # Set table field names based on specified return columns or all headers
                table.field_names = return_columns if return_columns else headers

                # If specific return columns are specified, find their indices
                if return_columns:
                    column_indices = [headers.index(col) for col in return_columns]

                for row in reader:
                    if row_limit is not None and row_count >= row_limit:
                        break

                    # Add only the specified columns to the table
                    if return_columns:
                        row_data = [row[i] for i in column_indices]
                        table.add_row(row_data)
                    else:
                        table.add_row(row)

                    row_count += 1

    print(table)
    return "Data displayed successfully."

