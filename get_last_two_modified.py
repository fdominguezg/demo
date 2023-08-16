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
    
    # Sort objects by LastModified in descending order
    objects.sort(key=itemgetter('LastModified'), reverse=True)
    
    # Retrieve the last two files and their upload dates
    if len(objects) >= 2:
        last_file = objects[0]
        second_last_file = objects[1]
        
        last_upload_date = last_file['LastModified']
        second_last_upload_date = second_last_file['LastModified']
        
        print("Last file upload date:", last_upload_date)
        print("Second last file upload date:", second_last_upload_date)
        
        # You can compare the upload dates here if needed
        if last_upload_date > second_last_upload_date:
            print("Last file was uploaded more recently.")
        else:
            print("Second last file was uploaded more recently.")
        
        # You can return these values if needed
        return {
            'last_upload_date': str(last_upload_date),
            'second_last_upload_date': str(second_last_upload_date)
        }
    else:
        print("Less than two files in the specified path.")
        return {
            'message': 'Less than two files in the specified path.'
        }
