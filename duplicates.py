import boto3
import csv
import io
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
    
    # Read the CSV file from S3 and process it in chunks
    input_file = s3_client.get_object(Bucket=bucket_name, Key=input_file_key)
    input_stream = input_file['Body']
    chunk_size = 10 * 1024 * 1024  # 10MB
    
    # Create a set to track unique rows
    unique_rows = set()
    
    # Create in-memory buffers for the cleaned files
    output_buffer = io.BytesIO()
    duplicates_buffer = io.BytesIO()
    
    # CSV writer for output and duplicates
    output_writer = csv.writer(output_buffer)
    duplicates_writer = csv.writer(duplicates_buffer)
    
    # Process the input CSV file in chunks
    while True:
        chunk = input_stream.read(chunk_size)
        if not chunk:
            break
        
        rows = chunk.decode('utf-8').split('\n')
        header = rows[0]
        
        # Process each row in the chunk
        for row in rows[1:]:
            if row:
                # Calculate the hash of the row excluding the "imei" column
                row_data = row.split(',')
                imei = row_data[1]  # Assuming "imei" is the second column
                hash_data = ','.join(row_data[:1] + row_data[2:])
                row_hash = hashlib.sha256(hash_data.encode('utf-8')).hexdigest()
                
                # Check if the hash is already in unique_rows set
                if row_hash in unique_rows:
                    # Duplicate found, write to duplicates file
                    duplicates_writer.writerow(row_data)
                else:
                    # Unique row, add hash to set and write to output file
                    unique_rows.add(row_hash)
                    output_writer.writerow(row_data)
    
    # Upload the cleaned CSV data to S3 (output with no duplicates)
    s3_client.put_object(Bucket=bucket_name, Key=output_file_key, Body=output_buffer.getvalue())
    
    # Upload the duplicates CSV data to S3
    s3_client.put_object(Bucket=bucket_name, Key=duplicates_file_key, Body=duplicates_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': f'Duplicates removed and files saved to S3. Original: {output_file_key}, Duplicates: {duplicates_file_key}'
    }
