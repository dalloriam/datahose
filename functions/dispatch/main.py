from dataclasses import dataclass, field

from google.cloud import storage, pubsub_v1

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

    @property
    def serialized(self) -> bytes:
        return json.dumps(self.dict).encode()


class EventSchema(Schema):
    key = fields.Str(required=True)
    body = fields.Dict(required=True)
    time = fields.Float(required=False)

    @post_load
    def make_event(self, data) -> Event:
        return Event(**data)


def fetch_mappings() -> dict:
    bucket_name = os.environ.get('BUCKET_NAME')
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    mapping_blob = bucket.get_blob('dispatch_mappings.json')
    mappings = json.loads(mapping_blob.download_as_string())

    return mappings


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
    request_json = request.get_json()

    schema = EventSchema()
    try:
        event: Event = schema.load(request_json)
    except Exception:
        return '{"error": "Bad Request."}', 400

    mappings = fetch_mappings()

    publisher: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient()

    for topic_name in mappings.get(event.key, []):
        topic_path = publisher.topic_path(project_name, topic_name)
        publisher.publish(topic_path, data=event.serialized)

    return json.dumps(event.dict)
