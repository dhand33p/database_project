import csv
import os

def handle_update_row(table_name, condition_column, operator, condition_value, update_column, update_value):
    base_directory = "table_chunks"
    table_directory = os.path.join(base_directory, table_name)

    if not os.path.exists(table_directory):
        return f"Table '{table_name}' does not exist."

    operators_mapping = {
        '<=': lambda x, y: x <= y,
        '>=': lambda x, y: x >= y,
        '==': lambda x, y: x == y,
        '!=': lambda x, y: x != y
    }

    for chunk_file in sorted(os.listdir(table_directory)):
        if chunk_file.endswith(".csv"):
            csv_file_path = os.path.join(table_directory, chunk_file)
            with open(csv_file_path, 'r') as csv_file:
                reader = csv.reader(csv_file)
                headers = next(reader)

                condition_column_index = headers.index(condition_column)
                update_column_index = headers.index(update_column)
                updated_rows = []

                for row in reader:
                    try:
                        cell_value = row[condition_column_index]

                        if isinstance(condition_value, str) and not condition_value.isdigit():
                            target_value = condition_value
                        else:
                            cell_value = float(cell_value)
                            target_value = float(condition_value)

                        if operators_mapping[operator](cell_value, target_value):
                            row[update_column_index] = update_value

                        updated_rows.append(row)

                    except (ValueError, IndexError):
                        continue

            with open(csv_file_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(updated_rows)

    return "Rows updated successfully."

