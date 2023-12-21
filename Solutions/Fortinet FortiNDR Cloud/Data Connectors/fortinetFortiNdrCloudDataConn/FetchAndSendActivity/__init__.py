import logging
import os
from datetime import datetime, timezone

from metastream import fetch_events
from metastream.s3_client import Context
from fortiNdrCloudRestAPI import _fetch_detections_checkpoint
from sentinel import post_data

AWS_ACCESS_KEY = os.environ.get('AwsAccessKeyId')
AWS_SECRET_KEY = os.environ.get('AwsSecretAccessKey')
ACCOUNT_CODE = os.environ.get("FncAccountCode")

def main(checkpoints: dict) -> str:
    new_checkpoints = {}
    for event_type, checkpoint in checkpoints.items():
        if not checkpoints:
            return ""

        logging.info(f'FetchAndSendActivity: event: {event_type} checkpoint: {checkpoint}')

        ctx = Context()
        start_date = datetime.fromisoformat(checkpoint).replace(tzinfo=timezone.utc)
        if event_type == 'detections':
            fetch_and_send_detections(ctx, event_type, start_date)
        else:
            fetch_and_send_events(ctx, event_type, start_date)

        if ctx.checkpoint is None:
            return ""
        new_checkpoints[event_type] = ctx.checkpoint.isoformat()

    return new_checkpoints


def fetch_and_send_events(ctx: Context, event_type: str, start_date: datetime):
    for events in fetch_events(context=ctx,
                               name='sentinel',
                               event_types=[event_type],
                               account_code=ACCOUNT_CODE,
                               start_date=start_date,
                               access_key=AWS_ACCESS_KEY,
                               secret_key=AWS_SECRET_KEY):
        post_data(events, event_type)


def fetch_and_send_detections(ctx: Context, event_type: str, start_date: datetime):
    detections = _fetch_detections_checkpoint(context = ctx, start_date = start_date)['detections']
    
    post_data(detections, 'detections')
