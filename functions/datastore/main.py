from google.cloud import datastore

import base64
import json


def ds_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    ds = datastore.Client()

    evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    key = ds.key('Event')

    entity = datastore.Entity(key=key)
    ds.put(entity)

    print(f'Processed event  [{evt["key"]}].')
