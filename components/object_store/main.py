from google.cloud import storage

import json
import os
import zlib


def obj_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.components.Context): Metadata for the event.
    """
    bucket_name = os.environ.get('BUCKET_NAME')
    storage_client = storage.Client()

    obj_id = event['key'].split('.', 1)[-1]

    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(obj_id).upload_from_string(
        zlib.compress(json.dumps(event['body']).encode()),
        content_type='application/zip'
    )

    print(f'Uploaded archive [{obj_id}]')
