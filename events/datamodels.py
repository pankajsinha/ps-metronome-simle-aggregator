import boto3

session = boto3.Session(profile_name='default')

dynamodb = session.client('dynamodb', endpoint_url='http://localhost:9000')

events_table_name = 'metronome-events'

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