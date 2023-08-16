import boto3
import csv
from difflib import ndiff

# Configurations
bucket_name1 = 'bucket-name-1'
file_key1 = 'path/to/file1.csv'
bucket_name2 = 'bucket-name-2'
file_key2 = 'path/to/file2.csv'
bucket_name_diff = 'bucket-name-differences'  # S3 bucket to store differences
diff_file_key = 'path/to/differences.csv'  # Key for storing differences file
chunk_size = 1000  # Adjust the chunk size as needed

def lambda_handler(event, context):
    # Create S3 clients
    s3 = boto3.client('s3')

    try:
        # Compare CSV data in chunks
        differences = compare_csv_in_chunks(s3, bucket_name1, file_key1, bucket_name2, file_key2)

        # Store the differences in an S3 bucket
        store_differences_in_s3(s3, bucket_name_diff, diff_file_key, differences)
        
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

def compare_csv_in_chunks(s3_client, bucket_name1, file_key1, bucket_name2, file_key2):
    s3 = boto3.client('s3')
    differences = []

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

        # Compare CSV data in this chunk
        chunk_differences = list(ndiff(csv_data1, csv_data2))
        differences.extend(chunk_differences)

    return differences

def store_differences_in_s3(s3_client, bucket_name_diff, diff_file_key, differences):
    # Create a CSV file with differences and write it to S3
    csv_content = '\n'.join(differences)
    
    s3_client.put_object(
        Bucket=bucket_name_diff,
        Key=diff_file_key,
        Body=csv_content.encode('utf-8'),
        ContentType='text/csv'
    )

# Uncomment and modify the configurations as needed
# lambda_handler(None, None)
