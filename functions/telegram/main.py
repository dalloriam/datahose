import base64
import json
import requests
import os


def _format(notification: dict) -> str:
    return f'*{notification.get("sender", "Unknown")}* - {notification["message"]}'


def send_message(body: dict):
    bot_key = os.environ.get('BOT_KEY')
    conv_id = os.environ.get('CONVERSATION_ID')

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
         context (google.cloud.functions.Context): Metadata for the event.
    """
    evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    send_message(evt['body'])
    print(f'Sent message to [{os.environ.get("CONVERSATION_ID")}]')
