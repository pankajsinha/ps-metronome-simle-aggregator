import sys
import csv
from datetime import datetime
from dateutil.parser import parse

import pytz

import service
from models import Event


def read_csv_file_and_ingest_events(csv_file_path: str) -> None:
    try:
        # Open the file for reading
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                # Remove trailing newline character and print the line
                print(row)

    except FileNotFoundError:
        print("File not found.")
    except IOError:
        print("Error reading the file.")


def convert_to_iso_8601_utc(timestamp: str) -> str:
    # Parse the timestamp string
    # dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f%z')
    dt = parse(timestamp)

    # Convert to UTC timezone
    utc_dt = dt.astimezone(pytz.UTC)

    # Format as ISO 8601 UTC timestamp
    iso_utc = utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    return iso_utc


def parse_event(event_row: list) -> Event:
    customer_id = event_row[0]
    event_type = event_row[1]
    transaction_id = event_row[2]
    ts_with_tz = event_row[3]
    ts_utc = convert_to_iso_8601_utc(ts_with_tz)

    return Event(
        customer_id=customer_id,
        event_type=event_type,
        transaction_id=transaction_id,
        ts=ts_utc
    )


def process_event(event: Event):
    try:
        service.persist_event(event)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    # Check if a csv file path is provided as a command-line argument
    if len(sys.argv) == 2:
        file_path = sys.argv[1]
        read_csv_file_and_ingest_events(file_path)
    else:
        print("Please provide only the events csv file path as a command-line argument.")
