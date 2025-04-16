import boto3
import io
import zipfile

# Main function to create and manage the S3 session
def main():
    # Create an S3 session
    session = boto3.Session(
        region_name='us-west-2',  # Replace with your desired AWS region
        aws_access_key_id='your_access_key',  # Replace with your AWS access key
        aws_secret_access_key='your_secret_key'  # Replace with your AWS secret access key
    )

    # Specify the source and destination bucket names and object keys
    source_bucket_name = 'source-bucket'
    source_object_key = 'example.zip'
    destination_bucket_name = 'destination-bucket'
    destination_object_key = 'example/'

    # Call the function to unzip and move the object
    unzip_and_move_object(session, source_bucket_name, source_object_key, destination_bucket_name, destination_object_key)

# Function to unzip an object and move it to another S3 bucket
def unzip_and_move_object(session, source_bucket_name, source_object_key, destination_bucket_name, destination_object_key):
    s3_client = session.client('s3')

    # Get the object from the source bucket as a stream
    obj = s3_client.get_object(Bucket=source_bucket_name, Key=source_object_key)
    file_stream = obj['Body']
    
    # Extract the zip file
    with zipfile.ZipFile(file_stream, 'r') as zip_file:
        for file_name in zip_file.namelist():
            # Extract the file from the zip
            extracted_file = zip_file.read(file_name)
            
            # Upload the extracted file to the destination bucket
            destination_object_name = destination_object_key + file_name
            s3_client.put_object(Body=extracted_file, Bucket=destination_bucket_name, Key=destination_object_name)
    
    # Delete the original zip file from the source bucket
    s3_client.delete_object(Bucket=source_bucket_name, Key=source_object_key)

# Call the main function if this module is being run as a standalone script
if __name__ == "__main__":
    main()

