from google.cloud import datastore, error_reporting

from datetime import datetime

import base64
import json


def ds_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.components.Context): Metadata for the event.
    """
    client = error_reporting.Client()
    try:
        ds = datastore.Client()

        evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))

        key = ds.key(f'Event-{evt["key"]}')

        entity = datastore.Entity(key=key)
        entity.update({
            **evt['body'],
            'key': evt['key'],
            'time': datetime.fromtimestamp(evt['time']),
            'dh_user_id': evt['user_id']
        })
        ds.put(entity)

        print(f'Processed event  [{evt["key"]}] for user [{evt["user_id"]}].')
    except Exception:
        client.report_exception()
