from collections import defaultdict
from typing import DefaultDict, List

from events import dao, datamodels
from events.models import BucketsRangeRequest, BucketsResponse, Event, Bucket

events_item_DAO = dao.ItemDAO(datamodels.events_table_name)


def persist_event(event: Event) -> None:
    item = event.to_dynamodb_item()
    events_item_DAO.upsert_item(item)


def get_buckets(request: BucketsRangeRequest) -> BucketsResponse:
    # Get all events_old for customer in the given time range.
    # TODO: Add pagination (i.e. Limit parameter for the dynamoDB query)
    items = events_item_DAO.get_items_by_partition_and_sort_key_range(
        request.customer_id, request.start, request.end
    )
    events = [Event.from_dynamodb_item(item) for item in items]
    # TODO: Use the sorted order of the results and just use current start_ts rather than the whole map to reduce memory
    #  footprint of maintaining the start_ts_to_buckets for a large time range.
    start_ts_to_buckets: DefaultDict[str, int] = defaultdict(int)
    for event in events:
        # Increment the start of the hour count for the event
        start_ts_to_buckets[event.get_start_of_the_hour()] += 1
    buckets: List[Bucket] = []
    for ts, count in start_ts_to_buckets.items():
        buckets.append(Bucket(ts_start_of_hour=ts, count=count))
    return BucketsResponse(customer_id=request.customer_id, buckets=buckets)



