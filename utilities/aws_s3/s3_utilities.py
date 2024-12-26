from io import StringIO
import pandas as pd
import boto3

def list_csv_files_in_folder(s3, bucket_name, folder_name):
    print(bucket_name, folder_name)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    print(response)
    csv_files = []

    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv'):  # Filter only CSV files
                csv_files.append(obj['Key'])

    return csv_files

def load_csv_to_dataframe(s3, bucket_name, file_key):
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')  # Decode the file content
    dataframe = pd.read_csv(StringIO(csv_content))  # Convert CSV content to DataFrame
    return dataframe