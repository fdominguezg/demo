import boto3
import math
import pandas as pd

s3 = boto3.resource('s3')
bucket_name = 'your-bucket-name'
source_key = 'source_directory/large_file.csv'  # Replace with the actual source path
target_directory = 'target_directory/'  # Replace with the actual target directory
chunk_size = 100 * 1024 * 1024  # 100MB in bytes

def lambda_handler(event, context):
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(source_key)
    total_bytes = obj.content_length
    total_chunks = int(math.ceil(total_bytes / chunk_size))
    
    header_line = None
    
    for chunk_number in range(1, total_chunks + 1):
        start_byte = (chunk_number - 1) * chunk_size
        end_byte = min(chunk_number * chunk_size - 1, total_bytes - 1)
        byte_range = f"bytes={start_byte}-{end_byte}"
        
        chunk_data = obj.get(Range=byte_range)['Body'].read()
        chunk_df = pd.read_csv(pd.compat.StringIO(chunk_data.decode('utf-8')))
        
        if header_line is None:
            header_line = chunk_data[:chunk_data.find(b'\n') + 1]
        
        target_key = f'{target_directory}small_chunk_{chunk_number}.csv'
        target_data = header_line + chunk_df.to_csv(index=False).encode('utf-8')
        
        s3.Object(bucket_name, target_key).put(Body=target_data)
