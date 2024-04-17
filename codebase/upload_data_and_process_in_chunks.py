import os
import csv
import time

def upload_data_and_process_in_chunks(file_path):
    def process_chunk(chunk, chunk_number, dataset_name, header):
        folder_path = os.path.join("table_chunks", dataset_name)
        os.makedirs(folder_path, exist_ok=True)

        csv_file_path = os.path.join(folder_path, f"{dataset_name}_chunk{chunk_number}.csv")
        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)
            writer.writerows(chunk)

    def read_in_chunks(file, chunk_size):
        file_ended = False

        while True:
            chunk = []
            total_size = 0

            while total_size < chunk_size:
                line = file.readline()

                if not line:
                    file_ended = True
                    break

                values = line.strip().split(',')
                chunk.append(values)
                total_size += len(line)

            if not chunk:
                break

            yield chunk

            if file_ended:
                break

    def calculate_chunk_size(file_size, max_chunk_size):
        if file_size < max_chunk_size:
            num_chunks = 10 
            return max(1, file_size // num_chunks)
        return max_chunk_size

    try:
        dataset_name = os.path.splitext(os.path.basename(file_path))[0]
        max_chunk_size = 25 * 1024 * 1024 

        file_size = os.path.getsize(file_path)
        optimal_chunk_size = calculate_chunk_size(file_size, max_chunk_size)

        start_time = time.time()

        with open(file_path, 'r') as file:
            header = file.readline().strip().split(',')

            chunk_generator = read_in_chunks(file, optimal_chunk_size)
            for chunk_number, chunk in enumerate(chunk_generator, start=1):
                process_chunk(chunk, chunk_number, dataset_name, header)

        end_time = time.time()

        return f"File '{file_path}' successfully processed in {end_time - start_time} seconds. Chunks saved in 'table_chunks/{dataset_name}_chunks' folder."

    except FileNotFoundError:
        return f"Failed to process the file: {file_path} not found."