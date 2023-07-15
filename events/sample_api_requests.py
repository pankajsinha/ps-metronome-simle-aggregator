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
    query_1_result = get_buckets_for_customer_for_30_years_range('1abb42414607955dbf6088b99f837d8f')
    print(query_1_result)

    # Run a buckets range query for 143k events customer
    query_2_result = get_buckets_for_customer_for_30_years_range('b4f9279a0196e40632e947dd1a88e857')
    print(query_2_result)

