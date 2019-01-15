from google.cloud import error_reporting, storage

import base64
import json
import os
import zlib


def fetch_config() -> dict:
    bucket_name = os.environ.get('CONFIG_BUCKET_NAME')
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    config_blob = bucket.get_blob('services/datahose.json')
    config = json.loads(config_blob.download_as_string())

    return config


config = fetch_config()


def obj_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.components.Context): Metadata for the event.
    """
    bucket_name = config['buckets']['archives']
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
