from twx.botapi import TelegramBot

import base64
import json
import os


def _format(notification: dict) -> str:
    return f'*{notification.get("sender", "Unknown")}* - {notification["message"]}'


def telegram_send(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bot_key = os.environ.get('BOT_KEY')
    conv_id = os.environ.get('CONVERSATION_ID')
    bot = TelegramBot(bot_key)

    evt = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    notification = evt['body']

    bot.send_message(conv_id, _format(notification), parse_mode='Markdown')
