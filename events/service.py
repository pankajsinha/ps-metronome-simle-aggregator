from collections import defaultdict
from typing import DefaultDict, List

import datamodels
from events import dao
from models import Event, BucketsResponse, BucketsRangeRequest, Bucket

events_item_DAO = dao.ItemDAO(datamodels.events_table_name)


def persist_event(event: Event) -> None:
    item = event.to_dynamodb_item()
    events_item_DAO.upsert_item(item)


def get_buckets(request: BucketsRangeRequest) -> BucketsResponse:
    # Get all events for customer in the given time range.
    items = events_item_DAO.get_items_by_partition_and_sort_key_range(
        request.customer_id, request.start, request.end
    )
    events = [Event.from_dynamodb_item(item) for item in items]
    start_ts_to_buckets: DefaultDict[str, int] = defaultdict(int)
    for event in events:
        # Increment the start of the hour count for the event
        start_ts_to_buckets[event.get_start_of_the_hour()] += 1
    buckets: List[Bucket] = []
    for ts, count in start_ts_to_buckets.items():
        buckets.append(Bucket(ts, count))
    return BucketsResponse(customer_id=request.customer_id, buckets=buckets)



