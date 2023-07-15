import re
from abc import ABC, abstractmethod
from typing import Dict, Type, Any, List
from typing import TypeVar
from dateutil.parser import parse

from pydantic import BaseModel, constr, Field

ISO_8601_UTC_TIMESTAMP_REGEX = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$'
iso_8601_pattern = re.compile(ISO_8601_UTC_TIMESTAMP_REGEX)

T = TypeVar('T', bound='DynamoDBItemInterface')


class DynamoDBItemInterface(ABC):
    @abstractmethod
    def to_dynamodb_item(self) -> Dict[str, Any]:
        pass

    @classmethod
    @abstractmethod
    def from_dynamodb_item(cls: Type[T], item: Dict[str, Any]) -> T:
        pass


class Event(BaseModel, DynamoDBItemInterface):
    customer_id: str
    event_type: str
    transaction_id: str
    ts: str = Field(pattern=iso_8601_pattern)

    def to_dynamodb_item(self) -> Dict:
        return {
            'partition_key': self.customer_id,
            'sort_key': f'{self.ts}-{self.event_type}-{self.transaction_id}',
            'customer_id': self.customer_id,
            'ts': self.ts,
            'event_type': self.event_type,
            'transaction_id': self.transaction_id,
        }

    @classmethod
    def from_dynamodb_item(cls, dynamodb_item):
        return cls(**dynamodb_item)

    def get_start_of_the_hour(self) -> str:
        # Parse the timestamp string
        dt = parse(self.ts)
        # set to start of the h
        start_of_the_hour = dt.replace(minute=0, second=0, microsecond=0)
        # Format as ISO 8601 UTC timestamp
        return start_of_the_hour.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


class BucketsRangeRequest(BaseModel):
    customer_id: str
    start: str = Field(pattern=iso_8601_pattern)
    end:  str = Field(pattern=iso_8601_pattern)


class Bucket(BaseModel):
    ts_start_of_hour: str = Field(pattern=iso_8601_pattern)
    count: int


class BucketsResponse(BaseModel):
    customer_id: str
    buckets: List[Bucket]
