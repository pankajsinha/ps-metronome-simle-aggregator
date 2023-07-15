from collections import defaultdict
from typing import DefaultDict, List

from events import dao, datamodels
from events.models import BucketsRangeRequest, BucketsResponse, Event, Bucket

events_item_DAO = dao.ItemDAO(datamodels.events_table_name)


def persist_event(event: Event) -> None:
    item = event.to_dynamodb_item()
    events_item_DAO.upsert_item(item)


def get_buckets(request: BucketsRangeRequest) -> BucketsResponse:
    buckets: List[Bucket] = []
    # Get all events_old for customer in the given time range.
    # TODO: Add pagination (i.e. Limit parameter for the dynamoDB query)
    last_evaluated_key = None
    cur_ts = None
    count = 0
    iteration = 0
    while True:
        iteration += 1
        items_result = events_item_DAO.get_items_by_partition_and_sort_key_range(
            request.customer_id, request.start, request.end, last_evaluated_key
        )

        for item in items_result.items:
            event = Event.from_dynamodb_item(item)
            ts = event.get_start_of_the_hour()
            if ts == cur_ts:
                count += 1
            else:
                # Flush the bucket since the current start of the hour ts is different
                if cur_ts is not None:
                    buckets.append(Bucket(ts_start_of_hour=cur_ts, count=count))
                cur_ts = ts
                count = 1

        last_evaluated_key = items_result.last_evaluated_key
        if last_evaluated_key is None:
            break
        print("Iteration: ", iteration)
        print("Num of items: ", len(items_result.items))
    if cur_ts is not None:
        buckets.append(Bucket(ts_start_of_hour=cur_ts, count=count))

    return BucketsResponse(customer_id=request.customer_id, buckets=buckets)



