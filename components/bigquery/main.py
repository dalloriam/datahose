from google.cloud import bigquery, error_reporting

from datetime import datetime

import base64
import os
import json

BIGQUERY_DATASET = os.environ['BIGQUERY_DATASET']
BIGQUERY_TABLE = os.environ['BIGQUERY_TABLE']


def bigquery_put(evt):
    bq = bigquery.Client()
    table = bq.get_table(bq.dataset(BIGQUERY_DATASET).table(BIGQUERY_TABLE))

    row = (evt['user_id'], datetime.fromtimestamp(evt['time']), evt['key'], json.dumps(evt['body']))

    bq.insert_rows(table, [row])


def bigquery_consume(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.components.Context): Metadata for the event.
    """
    client = error_reporting.Client()
    try:
        evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        bigquery_put(evt)
        print(f'Processed event  [{evt["key"]}] for user [{evt["user_id"]}].')

    except Exception:
        client.report_exception()
