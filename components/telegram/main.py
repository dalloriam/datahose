from google.cloud import error_reporting, storage


import base64
import json
import requests
import os


def fetch_config() -> dict:
    bucket_name = os.environ.get('CONFIG_BUCKET_NAME')
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    config_blob = bucket.get_blob('services/datahose.json')
    config = json.loads(config_blob.download_as_string())

    return config


config = fetch_config()


def _format(notification: dict) -> str:
    return f'*{notification.get("sender", "Unknown")}* - {notification["message"]}'


def send_message(body: dict):
    bot_key = config['telegram_notifier']['bot_key']
    conv_id = config['telegram_notifier']['conversation_id']

    formatted = _format(body)
    requests.post(
        url=f'https://api.telegram.org/bot{bot_key}/sendMessage',
        json={
            'chat_id': conv_id,
            'text': formatted,
            'parse_mode': 'Markdown'
        }
    )


def telegram_send(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.components.Context): Metadata for the event.
    """
    client = error_reporting.Client()

    try:
        evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        send_message(evt['body'])
        print(f'Sent message to [{config["telegram_notifier"]["conversation_id"]}]')
    except Exception:
        client.report_exception()
