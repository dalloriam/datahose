from dataclasses import dataclass, field

from marshmallow import fields, Schema

import json
import time


@dataclass
class Event:
    key: str       # Type of the event, useful for dispatching.
    body: dict      # Body of the event
    time: float = field(default_factory=lambda: time.time())  # When the event happened, useful for logging.

    @property
    def namespace(self) -> str:
        return self.key.split('.')[0]

    @classmethod
    def from_dict(cls, d):
        return Event(key=d['key'], body=d['body'], time=d['time'])

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
