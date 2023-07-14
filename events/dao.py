import boto3

session = boto3.Session(profile_name='default')
dynamodb_resource = session.resource('dynamodb', endpoint_url='http://localhost:9000')


class ItemDAO:

    def __init__(self, table_name):
        self.table = dynamodb_resource.Table(table_name)

    def upsert_item(self, item):
        self.table.put_item(Item=item)

    def get_item(self, partition_key, sort_key):
        response = self.table.get_item(Key={
            'partition_key': partition_key,
            'sort_key': sort_key
        })
        return response.get('Item')

    def get_items_by_partition_and_sort_key_range(self, partition_key, sort_key_start, sort_key_end):
        response = self.table.query(
            KeyConditionExpression='#pk = :pk_value AND #sk BETWEEN :sk_start AND :sk_end',
            ExpressionAttributeNames={
                '#pk': 'partition_key',
                '#sk': 'sort_key'
            },
            ExpressionAttributeValues={
                ':pk_value': partition_key,
                ':sk_start': sort_key_start,
                ':sk_end': sort_key_end
            },
            ScanIndexForward=True  # Sort the items in ascending order based on sort_key
        )

        items = response['Items']
        return items
