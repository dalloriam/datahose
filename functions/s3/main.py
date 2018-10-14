import base64
import boto3
import json
import os


def s3_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    current_body: str = ''
    s3 = boto3.resource('s3')
    try:
        current_body: str = s3.Object(os.environ.get('BUCKET_NAME'), evt['key']).get()['Body'].read().decode()
    except Exception as e:
        # TODO: Validate boto exception is 404
        pass
    current_body += json.dumps(evt) + '\n'

    s3.Object(os.environ.get('BUCKET_NAME'), evt['key']).put(Body=current_body)

    print(f'Processed event  [{evt["key"]}].')
