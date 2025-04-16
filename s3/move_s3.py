import boto3
import argparse

def move_files_between_buckets(source_bucket, destination_bucket):
    s3 = boto3.client('s3')
    
    # List the objects in the source bucket
    objects = s3.list_objects(Bucket=source_bucket)
    
    # Check if there are any objects in the source bucket
    if 'Contents' not in objects:
        print(f"No files found in the source bucket: {source_bucket}")
        return

    # Move each object to the destination bucket
    for obj in objects['Contents']:
        copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=obj['Key'])
        
        # Now delete the original object from the source bucket
        s3.delete_object(Bucket=source_bucket, Key=obj['Key'])
        print(f"Moved {obj['Key']} from {source_bucket} to {destination_bucket}")

if __name__ == "__main__":
    # Retrieve default buckets from environment variables
    DEFAULT_SOURCE_BUCKET = os.getenv('DEFAULT_SOURCE_BUCKET')
    DEFAULT_DESTINATION_BUCKET = os.getenv('DEFAULT_DESTINATION_BUCKET')
    
    parser = argparse.ArgumentParser(description="Move files from one S3 bucket to another.")
    parser.add_argument('--source', default=DEFAULT_SOURCE_BUCKET, help=f'Name of the source S3 bucket. Default: {DEFAULT_SOURCE_BUCKET}')
    parser.add_argument('--destination', default=DEFAULT_DESTINATION_BUCKET, help=f'Name of the destination S3 bucket. Default: {DEFAULT_DESTINATION_BUCKET}')
    
    args = parser.parse_args()
    
    move_files_between_buckets(args.source, args.destination)

    ### python script_name.py --source your_source_bucket_name --destination your_destination_bucket_name

