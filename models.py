from abc import ABC, abstractmethod
from typing import Dict, Type, Any
from typing import TypeVar

from pydantic import BaseModel, constr

ISO_8601_UTC_TIMESTAMP_REGEX = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$'

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
    ts: constr(regex=ISO_8601_UTC_TIMESTAMP_REGEX)

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
