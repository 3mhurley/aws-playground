import argparse
import logging
import boto3
from botocore.exceptions import ClientError
import decimal
import json
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def put_item(table, item) -> None:
    """
    Upload data to DynamoDB table.

    Args:
        table: dynamodb table resource
        item: record from the csv to be put
    """

    try:
        table.put_item(Item=item)

        print(f"Record put successfully.")

    except ClientError as e:
        print(f"DynamoDB error, {e.operation_name}-{e.response}")


def get_dynamo_table(dynamodb_resource, source_table):
    response = source_table.scan()

    data = []
    for item in response['Items']:
        def decimal_default(obj):
            if isinstance(obj, decimal.Decimal):
                if obj % 1 > 0:
                    return float(obj)
                else:
                    return int(obj)
            raise TypeError

        item = json.loads(json.dumps(item, default=decimal_default))

        data.append(item)

    return pd.DataFrame(data)


def csv(args):
    csv_data = pd.read_csv(args.file)

    # Upload configuration
    print(f"Uploading configuration - {args.file} to {args.table}")

    for linenumber, (index, row) in enumerate(csv_data.iterrows()):
        item = row.to_dict()

        print(f"Putting record number {linenumber+1}/{len(csv_data)}")

        for key in item:
            if key == "count":
                if not isinstance(item[key], int):
                    item[key] = int(item[key])
            elif not isinstance(item[key], str):
                item[key] = str(item[key])

        put_item(table, item)

    print(f"Configuration uploaded")

def main(args):
    """
    Main Function
        Initializes DynamoDB Session
        Puts each row from the configuration file to the requested dynamodb table

    Args:
        args: arguments from the command line
            - table: aws dynamodb table name
            - file: filepath for csv configuration file
            - region: aws region (defaults to us-east-1)
            - profile: aws cli profile (defaults to default)
    """
    # AWS Session
    print("Establishing AWS Session")
    
    session = boto3.Session(profile_name=args.profile)
    dynamodb_resource = session.resource('dynamodb', region_name=args.region)
    table = dynamodb_resource.Table(args.table)

    # Read configuration
    dynamo_data = get_dynamo_table(dynamodb_resource, table)

if __name__ == "__main__":
    """
    Configuration
    [
        {
            "source": "s3/source",
            "destination": "s3/landing",
            "column": "s3_location"
        },
        {
            "source": "s3/source",
            "destination": "s3/landing",
            "column": null
        }
    ]
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table",
        type=str,
        help="DynamoDB Configuration Table",
        required=True
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Path to Configuration File",
        required=True
    )
    parser.add_argument(
        "--region",
        type=str,
        help="AWS Region",
        default="us-east-1",
    )
    parser.add_argument(
        "--profile",
        type=str,
        help="AWS Credential Profile",
        default="default",
    )

    args = parser.parse_args()

    main(args)

