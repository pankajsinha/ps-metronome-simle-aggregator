from datetime import datetime, timedelta

import requests
from pydantic import BaseModel

from events.models import BucketsRangeRequest, BucketsResponse
from ingest_events_from_file import convert_to_iso_8601_utc


def get_buckets_range(bucket_range_request: BucketsRangeRequest) -> BucketsResponse:
    get_buckets_url = 'http://localhost:8800/events/customers/' + str(bucket_range_request.customer_id) + '/buckets'
    buckets_get_response = requests.get(get_buckets_url, data=bucket_range_request.json())
    assert buckets_get_response.status_code == 200
    return BucketsResponse(**buckets_get_response.json())


class GetBucketsQueryResult(BaseModel):
    response: BucketsResponse
    request: BucketsRangeRequest
    query_time: timedelta
    total_unique_events: int


def get_buckets_for_customer_for_30_years_range(customer_id: str) -> GetBucketsQueryResult:
    # Run a buckets range query for customer in the time range of 30 years ago
    current_date = datetime.now()
    date_30_years_ago = current_date - timedelta(days=30 * 365)
    date_30_years_ago_utc = convert_to_iso_8601_utc(date_30_years_ago.isoformat())
    current_date_utc = convert_to_iso_8601_utc(current_date.isoformat())
    request = BucketsRangeRequest(customer_id=customer_id, start=date_30_years_ago_utc, end=current_date_utc)
    response = get_buckets_range(
        request)
    result = GetBucketsQueryResult(response=response,
                                   request=request,
                                   query_time=datetime.now() - current_date,
                                   total_unique_events=get_total_count_of_events(response))
    return result


def get_total_count_of_events(buckets_response: BucketsResponse) -> int:
    return sum(bucket.count for bucket in buckets_response.buckets)


if __name__ == '__main__':
    # Run a buckets range query for 10k+ events customer in the time range of 30 years ago There are 143k events for
    # customer key b4f9279a0196e40632e947dd1a88e857 but it is only 10k returning 4k events response=BucketsResponse(
    # customer_id='b4f9279a0196e40632e947dd1a88e857', buckets=[Bucket(ts_start_of_hour='2021-03-01T00:00:00.000000Z',
    # count=2499), Bucket(ts_start_of_hour='2021-03-01T01:00:00.000000Z', count=658),
    # Bucket(ts_start_of_hour='2021-03-01T02:00:00.000000Z', count=185),
    # Bucket(ts_start_of_hour='2021-03-01T03:00:00.000000Z', count=661)]) request=BucketsRangeRequest(
    # customer_id='b4f9279a0196e40632e947dd1a88e857', start='1993-07-21T19:09:18.902350Z',
    # end='2023-07-14T19:09:18.902350Z') query_time=datetime.timedelta(seconds=1, microseconds=938837)
    # total_unique_events=4003
    query_1_result = get_buckets_for_customer_for_30_years_range('b4f9279a0196e40632e947dd1a88e857')
    print(query_1_result)
    query_2_result = get_buckets_for_customer_for_30_years_range('1abb42414607955dbf6088b99f837d8f')
    print(query_2_result)
