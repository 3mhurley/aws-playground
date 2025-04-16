import boto3

session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY',
    region_name='YOUR_REGION'
)

source_dynamodb = session.resource('dynamodb')
target_dynamodb = session.resource('dynamodb')

def copy_data(source_table_name, target_table_name):
    source_table = source_dynamodb.Table(source_table_name)
    target_table = target_dynamodb.Table(target_table_name)
    
    # This scan will get all the items from the source table
    # Note: If the table is very large, you'll need to handle paginated responses!
    response = source_table.scan()
    
    for item in response['Items']:
        # This puts each item into the target table
        # Note: If you have many items, consider using batch_write_item() for efficiency
        target_table.put_item(Item=item)
    
    # If there are more items in the source table, continue to fetch them
    while 'LastEvaluatedKey' in response:
        response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response['Items']:
            target_table.put_item(Item=item)

# Use the function
copy_data('SourceTableName', 'TargetTableName')
