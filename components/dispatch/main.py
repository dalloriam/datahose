from dalloriam.authentication.user import User
from dalloriam.exceptions import AccessDenied

from dataclasses import dataclass, field

from flask import Response

from google.cloud import error_reporting, pubsub_v1

from http import HTTPStatus

from marshmallow import fields, Schema, post_load

import json
import os
import time


@dataclass
class Event:
    key: str       # Type of the event, useful for dispatching.
    body: dict      # Body of the event
    time: float = field(default_factory=lambda: time.time())  # When the event happened, useful for logging.

    @property
    def namespace(self) -> str:
        return self.key.split('.')[0]

    @property
    def dict(self) -> dict:
        return {
            'key': self.key,
            'time': self.time,
            'body': self.body
        }


class EventSchema(Schema):
    key = fields.Str(required=True)
    body = fields.Dict(required=True)
    time = fields.Float(required=False)

    @post_load
    def make_event(self, data) -> Event:
        return Event(**data)


def authenticate(auth_header: str):
    if auth_header.startswith('Bearer'):
        # If we use bearer token, fetch the user info directly from there.
        return User.from_token(auth_header.split(' ').pop())

    elif auth_header.startswith('UPW'):
        # Custom format for passing username & password. Less secure.
        email, password = auth_header.split(' ')[-2:]
        uid, (id_token, refresh_token) = User.get_token(os.environ.get('FIREBASE_API_KEY'), email, password)
        return User.from_uid(uid)

    else:
        raise AccessDenied('Invalid header')


publisher: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient()


def dispatch(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/0.12/api/#flask.Flask.make_response>`.
    """
    project_name = os.environ.get('PROJECT_NAME')

    client = error_reporting.Client()
    try:
        request_json = request.get_json()

        if 'Authorization' in request.headers:
            auth_str = request.headers['Authorization']
        else:
            auth_str = request_json.get('auth', '')

        try:
            user = authenticate(auth_str)
        except AccessDenied as e:
            return Response(json.dumps({'error': str(e)}), HTTPStatus.FORBIDDEN, content_type='application/json')

        if 'datahose' not in user.services:
            return Response(
                json.dumps({'error': 'Datahose service not configured.'}),
                HTTPStatus.UNPROCESSABLE_ENTITY,
                content_type='application/json'
            )

        mappings = user.services['datahose']['namespace_topic_mappings']

        schema = EventSchema(strict=True)
        try:
            event: Event = schema.load(request_json).data
        except Exception:
            print(f'Got bad request: [{request_json}]')
            return '{"error": "Bad Request."}', HTTPStatus.BAD_REQUEST

        for topic_name in mappings.get(event.namespace, []):
            topic_path = publisher.topic_path(project_name, topic_name)
            publisher.publish(topic_path, data=json.dumps({**event.dict, 'user_id': user.uid}).encode())

    except Exception:
        client.report_exception()

    return Response(json.dumps(event.dict), content_type='application/json')
