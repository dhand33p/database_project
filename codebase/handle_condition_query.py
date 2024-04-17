import csv
import os
from prettytable import PrettyTable

def handle_condition_query(table_name, column_name, operator, value, return_columns=None, sort_by_column=None, row_limit=None):
    base_directory = "table_chunks"
    table_directory = os.path.join(base_directory, table_name)

    if not os.path.exists(table_directory):
        return f"Table '{table_name}' does not exist."

    result_table = PrettyTable()
    result_table.align = "l"

    matching_rows = []
    all_headers = set()
    operators_mapping = {
        '<=': lambda x, y: x <= y,
        '>=': lambda x, y: x >= y,
        '==': lambda x, y: x == y,
        '!=': lambda x, y: x != y,
        '<': lambda x, y: x < y,
        '>': lambda x, y: x > y
    }

    for chunk_file in sorted(os.listdir(table_directory)):
        if chunk_file.endswith(".csv"):
            csv_file_path = os.path.join(table_directory, chunk_file)
            with open(csv_file_path, 'r') as csv_file:
                reader = csv.DictReader(csv_file)
                headers = reader.fieldnames
                all_headers.update(headers)

                for row in reader:
                    cell_value = row[column_name]

                    # Adjust comparison for string or numeric values
                    is_string_comparison = not cell_value.replace('.', '', 1).isdigit()
                    if is_string_comparison:
                        target_value = value
                    else:
                        cell_value = float(cell_value)
                        target_value = float(value)

                    if operators_mapping[operator](cell_value, target_value):
                        if return_columns:
                            matching_row = [row[col] for col in return_columns]
                        else:
                            matching_row = [row[header] for header in headers]
                        matching_rows.append(matching_row)

    if sort_by_column and sort_by_column in headers:
        sort_column_index = headers.index(sort_by_column)
        matching_rows.sort(key=lambda row: row[sort_column_index])

    if row_limit is not None:
        matching_rows = matching_rows[:row_limit]

    result_table.field_names = return_columns if return_columns else list(all_headers)
    for row_values in matching_rows:
        result_table.add_row(row_values)

    return str(result_table)