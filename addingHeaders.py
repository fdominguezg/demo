import boto3
from operator import itemgetter

def lambda_handler(event, context):
    # Configure your S3 bucket and path details
    bucket_name = 'your-bucket-name'
    path_prefix = 'specific/path/'  # Specify the path prefix
    
    # Create an S3 client
    s3 = boto3.client('s3')

    # List objects in the specified path and get metadata
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)
    objects = response.get('Contents', [])
    
    # Filter out directories and retrieve the names of the last two files
    file_objects = [obj for obj in objects if not obj['Key'].endswith('/')]
    
    # Sort file objects by LastModified in descending order
    file_objects.sort(key=itemgetter('LastModified'), reverse=True)
    
    # Retrieve the names of the last two files
    if len(file_objects) >= 2:
        last_file_name = file_objects[0]['Key'].split('/')[-1]  # Extract the last part of the key as the file name
        second_last_file_name = file_objects[1]['Key'].split('/')[-1]  # Extract the last part of the key as the file name
        
        last_upload_date = file_objects[0]['LastModified']
        second_last_upload_date = file_objects[1]['LastModified']
        
        # Get the content of the last two files
        last_file_obj = s3.get_object(Bucket=bucket_name, Key=file_objects[0]['Key'])
        second_last_file_obj = s3.get_object(Bucket=bucket_name, Key=file_objects[1]['Key'])
        
        last_file_content = last_file_obj['Body'].read().decode('utf-8')
        second_last_file_content = second_last_file_obj['Body'].read().decode('utf-8')
        
        # Calculate the record differences and prepare the new content
        last_file_records = set(last_file_content.strip().split('\n'))
        second_last_file_records = set(second_last_file_content.strip().split('\n'))
        
        record_differences = last_file_records.symmetric_difference(second_last_file_records)
        
        header = "Record Differences between {} and {}\n".format(last_file_name, second_last_file_name)
        new_file_content = header + '\n'.join(record_differences)
        
        # Create a new file with differences and headers
        new_file_name = "differences_{}_{}.txt".format(last_file_name, second_last_file_name)
        s3.put_object(Bucket=bucket_name, Key=new_file_name, Body=new_file_content)
        
        print("New file with differences saved:", new_file_name)
        
        # Return relevant information if needed
        return {
            'last_file_name': last_file_name,
            'second_last_file_name': second_last_file_name,
            'last_upload_date': str(last_upload_date),
            'second_last_upload_date': str(second_last_upload_date),
            'record_differences': list(record_differences),
            'new_file_name': new_file_name
        }
    else:
        print("Less than two files in the specified path.")
        return {
            'message': 'Less than two files in the specified path.'
        }
