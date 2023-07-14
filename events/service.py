import boto3

import datamodels
from models import Event, BucketsResponse, BucketsRangeRequest

session = boto3.Session(profile_name='default')
dynamodb_resource = session.resource('dynamodb', endpoint_url='http://localhost:9000')
events_table = dynamodb_resource.Table(datamodels.events_table_name)


def persist_event(event: Event) -> None:
    item = event.to_dynamodb_item()
    events_table.put_item(Item=item)


def get_buckets(request: BucketsRangeRequest) -> BucketsResponse:
    pass
