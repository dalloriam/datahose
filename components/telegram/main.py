from dalloriam.authentication.user import User
from google.cloud import error_reporting


import base64
import json
import requests


def _format(notification: dict) -> str:
    return f'*{notification.get("sender", "Unknown")}* - {notification["message"]}'


def send_message(body: dict, conversation_id: str, bot_key: str):

    formatted = _format(body)
    requests.post(
        url=f'https://api.telegram.org/bot{bot_key}/sendMessage',
        json={
            'chat_id': conversation_id,
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
        user = User.from_uid(evt['user_id'])

        if 'telegram' not in user.services['datahose']:
            print(f'Telegram notifications disabled for user [{user.uid}]')
            return

        conversation_id = user.services['datahose']['telegram']['conversation_id']
        bot_key = user.services['datahose']['telegram']['bot_key']

        send_message(evt['body'], conversation_id, bot_key)
        print(f'Sent message to user [{user.uid}] on conversation [{conversation_id}]')
    except Exception:
        client.report_exception()
