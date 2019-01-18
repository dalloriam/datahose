from google.cloud import error_reporting, storage

import base64
import json
import os
import zlib


def obj_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.components.Context): Metadata for the event.
    """
    bucket_name = os.environ.get('ARCHIVES_BUCKET_NAME')
    client = error_reporting.Client()

    try:
        storage_client = storage.Client()

        evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))

        obj_id = evt['key'].split('.', 1)[-1]

        bucket = storage_client.get_bucket(bucket_name)
        bucket.blob(obj_id).upload_from_string(
            zlib.compress(json.dumps(evt['body']).encode()),
            content_type='application/zip'
        )

        print(f'Uploaded archive [{obj_id}]')
    except Exception:
        client.report_exception()
