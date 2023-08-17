import boto3
import io

def lambda_handler(event, context):
    # Set your AWS region
    region_name = 'YOUR_REGION'
    
    # Initialize the S3 client
    s3_client = boto3.client('s3', region_name=region_name)
    
    # Specify your S3 bucket and file paths
    bucket_name = 'YOUR_BUCKET_NAME'
    input_file_key = 'path/to/input.csv'
    output_file_key = 'path/to/cleaned.csv'
    
    # Read the CSV file from S3 in chunks of 10MB, clean the data, and save to a new CSV file
    input_file = s3_client.get_object(Bucket=bucket_name, Key=input_file_key)
    input_stream = input_file['Body']
    chunk_size = 10 * 1024 * 1024  # 10MB
    
    output_chunks = []
    cleaned_chunk = b''
    
    while True:
        chunk = input_stream.read(chunk_size)
        if not chunk:
            break
        
        cleaned_chunk += chunk.replace(b'\x00', b'')
        
        while b'\n' in cleaned_chunk:
            line, cleaned_chunk = cleaned_chunk.split(b'\n', 1)
            output_chunks.append(line + b'\n')
    
    # Combine the cleaned chunks and create a cleaned CSV file in memory as a string
    cleaned_data = b''.join(output_chunks)
    cleaned_csv = cleaned_data.decode('utf-8')
    
    # Upload the cleaned CSV data as a new file to S3
    s3 = boto3.client('s3', region_name=region_name)
    s3.put_object(Bucket=bucket_name, Key=output_file_key, Body=cleaned_csv)
    
    return {
        'statusCode': 200,
        'body': 'Cleaned data saved to S3 successfully.'
    }
