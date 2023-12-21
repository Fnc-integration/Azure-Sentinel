import logging
import os
from datetime import date, datetime, timedelta, timezone, time

from metastream import fetch_events_by_day
from metastream.s3_client import Context
from fortiNdrCloudRestAPI import _fetch_detections_by_checkpoint
from sentinel.sentinel import post_data

AWS_ACCESS_KEY = os.environ.get('AwsAccessKeyId')
AWS_SECRET_KEY = os.environ.get('AwsSecretAccessKey')
ACCOUNT_CODE = os.environ.get("FncAccountCode")
ACCOUNT_UUID = os.environ.get('FncAccountUUID')
API_TOKEN = os.environ.get('FncApiToken')

def main(args: dict) -> str:
    events = args.get('events', [])
    day: int = args.get('day')
    for event_type in events:
        ctx = Context()
        start_day = datetime.now(tz=timezone.utc) - timedelta(days=day)
        logging.info(f'FetchAndSendByDayActivity: event: {event_type} day: {start_day.date()}')

        if event_type == 'detections':
            fetch_and_send_detections(start_day.date())
        else:
            fetch_and_send_events(ctx, event_type, start_day)

    # it's required to return something that is json serializable
    return 'success'


def fetch_and_send_events(ctx: Context, event_type: str, start_day: datetime):
    for events in fetch_events_by_day(context=ctx,
                                      name='sentinel',
                                      event_type=event_type,
                                      account_code=ACCOUNT_CODE,
                                      day=start_day,
                                      access_key=AWS_ACCESS_KEY,
                                      secret_key=AWS_SECRET_KEY):
        post_data(events, event_type)



def fetch_and_send_detections(start_day: date):
    detections =  _fetch_detections_by_checkpoint(start_day = start_day)['detections']

    post_data(detections, 'detections')
