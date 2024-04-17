import csv
import os
import statistics
from prettytable import PrettyTable

def handle_group(table_name, group_column, return_specification):
    base_directory = "table_chunks"
    table_directory = os.path.join(base_directory, table_name)

    if not os.path.exists(table_directory):
        return f"Table '{table_name}' does not exist."

    aggregation_data = {}
    additional_columns = []

    if "ALL" not in return_specification:
        additional_columns = [col.strip() for col in return_specification if col != "ALL" and "(" not in col]

    for chunk_file in sorted(os.listdir(table_directory)):
        if chunk_file.endswith(".csv"):
            csv_file_path = os.path.join(table_directory, chunk_file)
            with open(csv_file_path, 'r') as csv_file:
                reader = csv.reader(csv_file)
                headers = next(reader)
                group_column_index = headers.index(group_column)
                additional_column_indices = [headers.index(col) for col in additional_columns]

                for row in reader:
                    key = row[group_column_index]
                    if key not in aggregation_data:
                        aggregation_data[key] = {"values": [], "additional": {col: set() for col in additional_columns}}

                    for func_name in return_specification:
                        if "(" in func_name and ")" in func_name:
                            func, col = func_name.replace(")", "").split("(")
                            col_index = headers.index(col)
                            cell_value = row[col_index]

                            if cell_value:
                                aggregation_data[key]["values"].append(float(cell_value))

                    for col, index in zip(additional_columns, additional_column_indices):
                        aggregation_data[key]["additional"][col].add(row[index])

    column_headers = [group_column]
    aggregated_columns = []  
    all_columns_included = "ALL" in return_specification

    for func_name in return_specification:
        if "(" in func_name and ")" in func_name:
            func, col = func_name.replace(")", "").split("(")
            aggregated_columns.append(col)
            if col != group_column:
                column_headers.append(f"{func}_of_{col}")

    if all_columns_included:
        for header in headers:
            if header not in column_headers and header not in aggregated_columns:
                column_headers.append(header)

    table = PrettyTable()
    table.field_names = column_headers
    table.align = "l"

    for key, data in aggregation_data.items():
        row = [key]
        for func_name in return_specification:
            if "(" in func_name and ")" in func_name:
                func, _ = func_name.replace(")", "").split("(")
                if data["values"]:
                    if func.lower() == "sum":
                        row.append(sum(data["values"]))
                    elif func.lower() == "mean":
                        row.append(statistics.mean(data["values"]))
                    elif func.lower() == "max":
                        row.append(max(data["values"]))
                    elif func.lower() == "count":
                        row.append(len(data["values"]))
                    elif func.lower() == "median":
                        row.append(statistics.median(data["values"]))
                else:
                    row.append(None)

        if all_columns_included:
            for col in headers:
                if col != group_column and col not in aggregated_columns:
                    additional_values = sorted(list(data["additional"][col]))
                    row.extend(additional_values)

        table.add_row(row)

    return table.get_string()
