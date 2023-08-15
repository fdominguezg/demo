import boto3
from difflib import ndiff

# Configurations
bucket_name1 = 'bucket-name-1'
file_key1 = 'path/to/file1.csv'
bucket_name2 = 'bucket-name-2'
file_key2 = 'path/to/file2.csv'
chunk_size = 1000  # Adjust the chunk size as needed

def lambda_handler(event, context):
    # Create S3 clients
    s3 = boto3.client('s3')

    try:
        # Compare CSV data and find non-existing records in chunks
        non_existing_records = find_non_existing_records(s3, bucket_name1, file_key1, bucket_name2, file_key2)

        # Store or output the non-existing records
        store_non_existing_records(non_existing_records)
        
    except Exception as e:
        print("Error:", e)
        # Handle the error

    return {
        'statusCode': 200,
        'body': 'CSV comparison completed.'
    }

def read_csv_chunk(s3_client, bucket_name, file_key, start_byte, end_byte):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key, Range=f'bytes={start_byte}-{end_byte}')
    body = response['Body'].read().decode('utf-8')
    return body.splitlines()

def find_non_existing_records(s3_client, bucket_name1, file_key1, bucket_name2, file_key2):
    s3 = boto3.client('s3')
    non_existing_records = []

    # Get file sizes for chunking
    response1 = s3.head_object(Bucket=bucket_name1, Key=file_key1)
    response2 = s3.head_object(Bucket=bucket_name2, Key=file_key2)
    total_size1 = response1['ContentLength']
    total_size2 = response2['ContentLength']

    # Compare CSV data in chunks
    for start_byte in range(0, total_size1, chunk_size):
        end_byte = min(start_byte + chunk_size - 1, total_size1 - 1)
        csv_data1 = read_csv_chunk(s3_client, bucket_name1, file_key1, start_byte, end_byte)

        end_byte2 = min(start_byte + chunk_size - 1, total_size2 - 1)
        csv_data2 = read_csv_chunk(s3_client, bucket_name2, file_key2, start_byte, end_byte2)

        # Find non-existing records in this chunk
        non_existing_chunk = [line for line in csv_data1 if line not in csv_data2]
        non_existing_records.extend(non_existing_chunk)

    return non_existing_records

def store_non_existing_records(non_existing_records):
    # Store or output the non-existing records as needed
    # For example, you can log them or save to an S3 bucket or a database
    pass
