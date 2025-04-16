import boto3
import json


# Export to json
def export_dynamodb_to_json(table_name, output_filename):
    # Initialize a session using Amazon DynamoDB
    session = boto3.Session()
    dynamodb = session.resource('dynamodb')
    
    # Initialize DynamoDB table resource
    table = dynamodb.Table(table_name)

    # Use the scan operation
    response = table.scan()
    items = response['Items']

    # Pagination for large tables
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    # Write to a JSON file
    with open(output_filename, 'w') as outfile:
        json.dump(items, outfile, indent=4, default=str)

    print(f"Exported {len(items)} items to {output_filename}")

# if __name__ == '__main__':
#     table_name = 'YourTableName'
#     output_filename = 'output.json'
#     export_dynamodb_to_json(table_name, output_filename)


# Export to s3
def start_export_to_s3(table_arn, s3_bucket_arn):
    client = boto3.client('dynamodb')
    
    response = client.export_table_to_point_in_time(
        TableArn=table_arn,
        S3BucketArn=s3_bucket_arn,
        ExportTimeFormat='ION'
    )
    
    return response['ExportDescription']

table_arn = 'arn:aws:dynamodb:REGION:ACCOUNT_ID:table/TABLE_NAME'
s3_bucket_arn = 'arn:aws:s3:::BUCKET_NAME'
export_description = start_export_to_s3(table_arn, s3_bucket_arn)
print(export_description)


# Delete and Rebuild
dynamodb = boto3.resource('dynamodb')
table_name = 'YourDestinationTableName'

# Delete the table
table = dynamodb.Table(table_name)
table.delete()
table.wait_until_not_exists()  # Wait until table is deleted

# Recreate the table with the same schema
# Note: Adjust attribute definitions, key schema, and other parameters as needed
table = dynamodb.create_table(
    TableName=table_name,
    AttributeDefinitions=[
        {
            'AttributeName': 'YourPrimaryKeyName',
            'AttributeType': 'S'  # 'S' for string. Use 'N' for number if needed
        }
    ],
    KeySchema=[
        {
            'AttributeName': 'YourPrimaryKeyName',
            'KeyType': 'HASH'  # Primary key type. Use 'RANGE' for sort keys if needed
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

table.wait_until_exists()  # Wait until table is created


# Delete items
dynamodb = boto3.resource('dynamodb')
table_name = 'YourDestinationTableName'
table = dynamodb.Table(table_name)

# Scan the table to get the primary keys
response = table.scan(ProjectionExpression='YourPrimaryKeyName')  # Adjust the projection expression if you have a composite primary key (HASH + RANGE)
items = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ProjectionExpression='YourPrimaryKeyName', ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response['Items'])

# Batch delete items
for item in items:
    table.delete_item(Key=item)

