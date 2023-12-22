import os
from datetime import date, datetime, timedelta, timezone, time
import requests
from typing import List, Any, Dict

from metastream.s3_client import Context
from metastream import _validate_start_date

ACCOUNT_UUID = os.environ.get('FncAccountUUID')
API_TOKEN = os.environ.get('FncApiToken')
MAX_DETECTIONS = 1000
POLLING_DELAY = int(os.environ.get("FncPollingDelay") or "10")

def _process_response(response: Dict[str, Any]):
    # Crete detections list
    detections = []
    rules = {}
    total_count = response['total_count'] if 'total_count' in response else -1
            
    # Put pulled detections and rules into lists
    for detection in response['detections']:
        detections.append(detection)
        
    for rule in response['rules']:
        if not rule['uuid'] in rules:
            rules[rule['uuid']] = rule
    
    return {
        'detections': detections,
        'rules': rules,
        'total_count': total_count
    }
    
def _add_detection_rule(detection: Dict[str, Any], rules: Dict[str, Any]):
    """ Create a new detection rule.
    """
    # Find the detection's rule in the dictionary and update the detection
    rule = rules[detection['rule_uuid']]

    detection.update({'rule_name': rule['name']})
    detection.update({'rule_severity': rule['severity']})
    detection.update({'rule_confidence': rule['confidence']})
    detection.update({'rule_category': rule['category']})


def _fetch_detections_inc(start: date, end: date, offset: int = 0) -> Dict[str, Any]:
    headers = {
        "Authorization": 'IBToken ' + API_TOKEN,
        "content-type": 'application/json'
    }
    url = 'https://detections.icebrg.io/v1/detections'
    args = {
        "account_uuid": ACCOUNT_UUID,
        "created_or_shared_start_date": start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        "created_or_shared_end_date": end.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        "include": 'rules,indicators',
        "limit": MAX_DETECTIONS,
        "offset": offset
        
    }
    first = True
    multiple_values=['include']
    for arg in args:
        values: List[Any] = []
        if arg in multiple_values:
            values.extend(args[arg].split(','))
        else:
            values.append(args[arg])

        for value in values:
            this_arg = str(arg) + "=" + str(value).strip()
            if first:
                url = url + "?" + this_arg
                first = False
            else:
                url = url + "&" + this_arg
    result = requests.get(url, headers=headers)
        
    r_json = result.json()
    return _process_response(response = r_json)
    
def _fetch_detections(start_day: date, end_day: date):
    result = {
        'total_count': -1,
        'detections': [],
        'rules': {}
    }
    
    offset = 0
    total_count = 0
    
    while result['total_count'] < 0 or offset < result['total_count']:
        next_piece = _fetch_detections_inc(start = start_day, end = end_day, offset = offset)
        
        if result['total_count'] < 0:
            result['total_count'] = next_piece['total_count']

        result['detections'].extend(next_piece['detections'])
        result['rules'] = dict(next_piece['rules'], **result['rules'])
        offset += MAX_DETECTIONS
        
        total_count += len(next_piece['detections'])
    
    for detection in result['detections']:
        _add_detection_rule(detection = detection, rules = result['rules'])

    return result

def _fetch_detections_by_day(start_day: date):
    result = _fetch_detections(start_day = start_day, end_day = datetime.combine(start_day, time.max))
    return result
    

def _fetch_detections_checkpoint(context: Context, start_date: date):
    checkpoint = (datetime.now(tz=timezone.utc) - timedelta(minutes=POLLING_DELAY)).replace(microsecond=0)
    if context:
        context.checkpoint = checkpoint
    
    _validate_start_date(start_date = start_date, checkpoint = checkpoint)
    
    result = _fetch_detections(start_day = start_date, end_day = checkpoint)
    
    return result
    
    