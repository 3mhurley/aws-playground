import boto3

def copy_dynamodb_table(source_table_name, dest_table_name):
    # Create boto3 client for DynamoDB
    dynamodb = boto3.resource('dynamodb')
    
    # Reference to source and destination tables
    source_table = dynamodb.Table(source_table_name)
    dest_table = dynamodb.Table(dest_table_name)

    # Scan source table
    response = source_table.scan()
    items = response['Items']

    # While there are more items to be fetched from source table
    while 'LastEvaluatedKey' in response:
        response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    # Batch write items to destination table
    # with dest_table.batch_writer() as batch:
    #     for item in items:
    #         batch.put_item(Item=item)
    with dest_table.batch_writer() as batch:
        for item in items:
            # Check if 'destination_location' attribute is present and modify its value
            if 'destination_location' in item:
                item['destination_location'] = item['destination_location'].replace('-prod', '-dev')
            
            batch.put_item(Item=item)

    print(f"Copied {len(items)} items from {source_table_name} to {dest_table_name}.")

if __name__ == '__main__':
    source_table_name = 'YourSourceTableName'
    dest_table_name = 'YourDestinationTableName'
    
    copy_dynamodb_table(source_table_name, dest_table_name)

