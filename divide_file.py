import boto3
import math
import pandas as pd
import io

s3 = boto3.resource('s3')
bucket_name = 'your-bucket-name'
source_key = 'source_directory/large_file.csv'  # Replace with the actual source path
target_directory = 'target_directory/'  # Replace with the actual target directory
target_chunk_size = 100 * 1024 * 1024  # 100MB in bytes (target chunk size)

def split_csv_into_chunks(file_content, chunk_size):
    lines = file_content.split(b'\n')
    chunks = []
    current_chunk = b""
    
    for line in lines:
        if len(current_chunk) + len(line) + 1 > chunk_size:
            chunks.append(current_chunk)
            current_chunk = b""
        current_chunk += line + b'\n'
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

def lambda_handler(event, context):
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(source_key)
    
    file_content = obj.get()['Body'].read()
    chunks = split_csv_into_chunks(file_content, target_chunk_size)
    
    header_line = chunks[0][:chunks[0].find(b'\n') + 1]
    
    for chunk_number, chunk_data in enumerate(chunks, start=1):
        chunk_df = pd.read_csv(io.StringIO(chunk_data.decode('utf-8')))
        
        target_key = f'{target_directory}small_chunk_{chunk_number}.csv'
        target_data = header_line + chunk_df.to_csv(index=False).encode('utf-8')
        
        s3.Object(bucket_name, target_key).put(Body=target_data)
