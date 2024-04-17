import os

def list_tables():
    base_directory = "table_chunks"
    tables = []

    if not os.path.exists(base_directory):
        return "No tables found. The directory does not exist."

    for entry in os.listdir(base_directory):
        if os.path.isdir(os.path.join(base_directory, entry)) and not entry.startswith('.'):
            tables.append(entry)

    return tables if tables else "No tables found."