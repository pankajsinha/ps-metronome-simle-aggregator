import sys
import csv
from datetime import datetime
from dateutil.parser import parse

import pytz

from events import service
from events.models import Event


def read_csv_file_and_ingest_events(csv_file_path: str) -> None:
    try:
        start_time = datetime.now()
        print(f"Starting processing at time: {start_time}")
        # Open the file for reading
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            for line_num, row in enumerate(csv_reader, start=1):
                # 1. Parse the event
                event = parse_event(row, line_num)
                # 2. Process the event
                if event is not None:
                    process_event(event, line_num)

                if line_num % 1000 == 0:
                    print(f"Processed {line_num} events.")
            end_time = datetime.now()
            print(f"Successfully processed all {line_num} events. Completed at {end_time}")
            print(f"Total processing time: {end_time- start_time}")

    except FileNotFoundError:
        print("File not found.")
    except IOError:
        print("Error reading the file.")


def convert_to_iso_8601_utc(timestamp: str) -> str:
    # Parse the timestamp string
    dt = parse(timestamp)

    # Convert to UTC timezone
    utc_dt = dt.astimezone(pytz.UTC)

    # Format as ISO 8601 UTC timestamp
    iso_utc = utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    return iso_utc


def parse_event(event_row: list, line_num: int) -> Event:
    try:
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
    except ValueError as e:
        # 3. Parsing errors
        print("Error parsing event at line {}: {}".format(line_num, event_row))
        print("Error: {}".format(e))


def process_event(event: Event, line_num: int) -> None:
    try:
        service.persist_event(event)
    except Exception as e:
        print("Error processing event at line {}: {}".format(line_num, event))
        print("Error: {}".format(e))


if __name__ == "__main__":
    # Check if a csv file path is provided as a command-line argument
    if len(sys.argv) == 2:
        file_path = sys.argv[1]
        read_csv_file_and_ingest_events(file_path)
    else:
        print("Please provide only the events csv file path as a command-line argument.")
