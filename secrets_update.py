import boto3
import json

def lambda_handler(event, context):
    # Replace 'your-secret-name' with the name of your secret in AWS Secrets Manager
    secret_name = 'your-secret-name'

    # Create an EKS client
    eks_client = boto3.client('eks')
    
    # Replace 'your-cluster-name' with the name of your EKS cluster
    cluster_name = 'your-cluster-name'

    try:
        # Describe the EKS cluster to get the ARN
        response = eks_client.describe_cluster(name=cluster_name)
        cluster_arn = response['cluster']['arn']

        # Create a Secrets Manager client
        secrets_manager = boto3.client('secretsmanager')

        # Retrieve the existing secret value
        get_secret_response = secrets_manager.get_secret_value(SecretId=secret_name)
        current_secret_value = json.loads(get_secret_response['SecretString'])

        # Modify the secret value (you can customize this part)
        new_secret_value = current_secret_value
        new_secret_value['key_to_modify'] = 'new_value'

        # Update the secret in Secrets Manager
        update_secret_response = secrets_manager.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(new_secret_value)
        )

        return {
            'statusCode': 200,
            'body': 'Secret updated successfully'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
