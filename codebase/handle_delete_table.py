import os
import shutil

def handle_delete_table(table_name):
    table_directory = os.path.join("table_chunks", table_name)

    if not os.path.exists(table_directory):
        return f"Table '{table_name}' does not exist and cannot be deleted."

    try:
        shutil.rmtree(table_directory)
        return f"Table '{table_name}' has been successfully deleted."
    except Exception as e:
        return f"An error occurred while deleting the table '{table_name}': {e}"
