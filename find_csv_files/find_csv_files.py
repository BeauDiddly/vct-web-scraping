import os

def find_csv_files(directory, folder_name, year):
    csv_files = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(f"vct_{year}/{folder_name}/{file}")
    return csv_files

