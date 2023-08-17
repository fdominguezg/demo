import boto3
import pandas as pd
import io
import csv
import hashlib

def lambda_handler(event, context):
    # Set your AWS region
    region_name = 'YOUR_REGION'
    
    # Initialize the S3 client
    s3_client = boto3.client('s3', region_name=region_name)
    
    # Specify your S3 bucket and file paths
    bucket_name = 'YOUR_BUCKET_NAME'
    input_file_key = 'path/to/input.csv'
    output_file_key = 'path/to/output_no_duplicates.csv'
    duplicates_file_key = 'path/to/duplicates.csv'
    
    # Read the CSV file from S3
    input_file = s3_client.get_object(Bucket=bucket_name, Key=input_file_key)
    input_stream = input_file['Body']
    
    # Load the first chunk of CSV data to identify the "imei" column
    first_chunk = input_stream.read(1024 * 1024)  # Read 1MB for header detection
    first_chunk_str = first_chunk.decode('utf-8')
    
    # Find the index of the "imei" column
    imei_column_idx = None
    header_row = first_chunk_str.split('\n', 1)[0].split(',')
    for idx, column in enumerate(header_row):
        if column.lower() == 'imei':
            imei_column_idx = idx
            break
    
    if imei_column_idx is None:
        return {
            'statusCode': 400,
            'body': 'No "imei" column found in the CSV file.'
        }
    
    # Process the input CSV file in chunks
    chunk_size = 10000  # Adjust the chunk size based on your memory and performance requirements
    chunk_iter = pd.read_csv(input_stream, chunksize=chunk_size)
    
    # Create in-memory buffers for the cleaned files
    output_buffer = io.StringIO()
    duplicates_buffer = io.StringIO()
    
    for chunk_df in chunk_iter:
        # Identify and remove duplicates based on the "imei" column
        duplicates = chunk_df[chunk_df.duplicated(subset=[header_row[imei_column_idx]], keep=False)]
        unique = chunk_df.drop_duplicates(subset=[header_row[imei_column_idx]])
        
        # Write the dataframes to the buffers
        unique.to_csv(output_buffer, index=False, header=False)
        duplicates.to_csv(duplicates_buffer, index=False, header=False)
    
    # Upload the cleaned CSV data to S3 (output with no duplicates)
    s3_client.put_object(Bucket=bucket_name, Key=output_file_key, Body=output_buffer.getvalue())
    
    # Upload the duplicates CSV data to S3
    s3_client.put_object(Bucket=bucket_name, Key=duplicates_file_key, Body=duplicates_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': f'Duplicates removed and files saved to S3. Original: {output_file_key}, Duplicates: {duplicates_file_key}'
    }
