import base64
import json


def s3_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    print(f'Got event  [{evt["key"]}]')
