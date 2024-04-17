import csv
import os
import time

def efficient_hash_join(folder1, folder2, join_column1, join_column2, output_folder):

    def build_hash_table(folder, join_column):
        hash_table = {}
        for file in os.listdir(folder):
            if file.endswith(".csv"):
                with open(os.path.join(folder, file), 'r') as csv_file:
                    reader = csv.DictReader(csv_file)
                    for row in reader:
                        key = row[join_column]
                        if key not in hash_table:
                            hash_table[key] = []
                        hash_table[key].append(row)
        return hash_table

    def stream_and_join(folder, hash_table, writer, join_column):
        for file in os.listdir(folder):
            if file.endswith(".csv"):
                with open(os.path.join(folder, file), 'r') as csv_file:
                    reader = csv.DictReader(csv_file)
                    for row in reader:
                        key = row[join_column]
                        if key in hash_table:
                            for hash_row in hash_table[key]:
                                joined_row = {**hash_row, **row}
                                writer.writerow(joined_row)


    def get_fieldnames(folder1, folder2, join_column1, join_column2):
        fieldnames = set()
        for folder, join_column in [(folder1, join_column1), (folder2, join_column2)]:
            for file in os.listdir(folder):
                if file.endswith(".csv"):
                    with open(os.path.join(folder, file), 'r') as csv_file:
                        reader = csv.DictReader(csv_file)
                        fieldnames.update(reader.fieldnames)
        return list(fieldnames)

    start_time = time.time()

    folder1_count = len([name for name in os.listdir(folder1) if name.endswith(".csv")])
    folder2_count = len([name for name in os.listdir(folder2) if name.endswith(".csv")])

    if folder1_count <= folder2_count:
        hash_table = build_hash_table(folder1, join_column1)
        streaming_folder = folder2
        streaming_column = join_column2
    else:
        hash_table = build_hash_table(folder2, join_column2)
        streaming_folder = folder1
        streaming_column = join_column1

    fieldnames = get_fieldnames(folder1, folder2, join_column1, join_column2)

    output_chunk_name = f"{os.path.basename(output_folder)}_chunk"
    
    os.makedirs(output_folder, exist_ok=True)

    chunk_counter = 1
    while True:
        output_file = os.path.join(output_folder, f"{output_chunk_name}{chunk_counter}.csv")
        if not os.path.exists(output_file):
            break
        chunk_counter += 1

    with open(output_file, 'w', newline='') as output_csv:
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()
        stream_and_join(streaming_folder, hash_table, writer, streaming_column)

    total_runtime = time.time() - start_time
    print(f"Total Runtime: {total_runtime} seconds")
