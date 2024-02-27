import os

def find_csv_files(directory, year):
    csv_files = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(f"vct_{year}/matches/{file}")
    return csv_files

