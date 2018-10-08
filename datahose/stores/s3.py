from collections import defaultdict

from datahose.event import Event
from datahose.scheduler import Scheduler
from datahose.stores.store import MetricStore

from datetime import timedelta

from typing import Dict, Iterator, List

import boto3
import json


class S3Store(MetricStore):

    def __init__(self, bucket_name: str, prefix: str=''):
        self._s3 = boto3.resource('s3')

        self._bucket_name = bucket_name

        if prefix and not prefix.endswith('/'):
            prefix += '/'
        self._prefix = prefix

        # TODO: Maybe use a shelve as a buffer instead of in-memory
        self._buffer: Dict[str, List[Event]] = defaultdict(lambda: [])

        Scheduler.schedule_repeat(self.flush, timedelta(minutes=1))

    def flush(self) -> int:
        # TODO: Proper logging
        i = len(self._buffer.keys())
        print(f'flushing cache with {i} keys.')

        for key, vals in self._buffer.items():
            current_body: str = ''
            try:
                current_body: str = self._s3.Object(self._bucket_name, self._prefix + key).get()['Body'].read().decode()
            except Exception as e:
                # TODO: Validate boto exception is 404
                pass

            for v in vals:
                current_body += json.dumps(v.serialized) + '\n'

            self._s3.Object(self._bucket_name, self._prefix + key).put(Body=current_body)

        self._buffer = defaultdict(lambda: [])
        print(f'cache flush completed successfully')
        return i

    def put(self, event: Event):
        self._buffer[event.key].append(event)

    def last(self, key: str):
        return None

    def __getitem__(self, key: str) -> Iterator[Event]:
        try:
            lines: List[str] = self._s3.Object(self._bucket_name,
                                                self._prefix + key).get()['Body'].read().decode().split('\n')
        except Exception as e:
            # TODO: Validate boto exception
            print(f'Got error while flushing cache: {e}')

        else:
            for line in lines:
                yield Event.from_dict(json.loads(line))
