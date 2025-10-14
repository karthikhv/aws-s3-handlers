import json
import urllib.parse
import boto3

# Initialize the S3 client outside the handler to improve performance 
# on subsequent calls (re-use the connection).
print('Loading function')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda function triggered by an S3 object creation event (PUT/POST).

    The function reads the bucket and key (filename) from the event 
    and retrieves the object's metadata (specifically the Content-Type).

    :param event: AWS S3 Event object containing information about the file upload.
    :param context: Lambda runtime information object.
    :return: The Content-Type (MIME type) of the uploaded S3 object.
    """
    
    # --- 1. Extract Bucket and Key (File Name) from the S3 Event ---
    
    # We only care about the first record in the event list (usually only one).
    s3_record = event['Records'][0]['s3']
    
    bucket = s3_record['bucket']['name']
    
    # The key (filename) often contains URL-encoded characters (like spaces), 
    # so we must unquote it to get the actual file path.
    key = urllib.parse.unquote_plus(s3_record['object']['key'], encoding='utf-8')
    
    print(f"Processing object: {key} from bucket: {bucket}")

    # --- 2. Retrieve Object Metadata from S3 ---
    try:
        # Use get_object to fetch metadata (like ContentType) without downloading 
        # the potentially large file body.
        response = s3.get_object(Bucket=bucket, Key=key)
        
        content_type = response['ContentType']
        
        # --- 3. Log Output ---
        print(f"SUCCESS: Content Type is: {content_type}")
        
        # The result is returned to the service that invoked the Lambda (CloudWatch Logs/S3).
        return content_type
        
    except Exception as e:
        # Handle errors (e.g., file doesn't exist, permission denied, region mismatch)
        print(e)
        print(f'ERROR: Could not get object {key} from bucket {bucket}. Check permissions and region.')
        raise e
