import os

import boto3
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

session = boto3.Session(profile_name='default')

DYNAMO_ENDPOINT_URL = os.getenv("DYNAMODB_ENDPOINT_URL")
dynamodb = session.client('dynamodb', endpoint_url=DYNAMO_ENDPOINT_URL)

events_table_name = os.getenv("DYNAMODB_EVENTS_TABLE_NAME")

events_table_schema = {
    'TableName': events_table_name,
    'KeySchema': [
        {'AttributeName': 'partition_key', 'KeyType': 'HASH'},  # customer_id
        {'AttributeName': 'sort_key', 'KeyType': 'RANGE'}  # Composite Key of ts, event_type and transaction_id
    ],
    'AttributeDefinitions': [
        {'AttributeName': 'partition_key', 'AttributeType': 'S'},
        {'AttributeName': 'sort_key', 'AttributeType': 'S'}
        # FYI: Will add all these attributes for faster access and provide more flexibility (for filtering)
        # {'AttributeName': 'customer_id', 'AttributeType': 'S'},
        # {'AttributeName': 'ts', 'AttributeType': 'S'},
        # {'AttributeName': 'event_type', 'AttributeType': 'S'},
        # {'AttributeName': 'transaction_id', 'AttributeType': 'S'},
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
}

try:
    table = dynamodb.create_table(**events_table_schema)
    print('Created table {}'.format(table['TableDescription']['TableName']))
except dynamodb.exceptions.ResourceInUseException:
    print('Metronome Events Table already exists')