from datahose.event import Event

from typing import Dict, Iterator

from datahose.stores.store import MetricStore

import json
import os


class DiskStore(MetricStore):

    def __init__(self, base_path: str):
        self._base_path = base_path

        self._cache: Dict[str, Event] = {}

    def flush(self):
        pass

    def put(self, event: Event) -> None:
        self._cache[event.key] = event

        with open(os.path.join(self._base_path, event.key), 'a') as outfile:
            outfile.write(json.dumps(event.serialized) + '\n')

    def __getitem__(self, key: str) -> Iterator[Event]:
        with open(os.path.join(self._base_path, key), 'r') as infile:
            line = infile.readline()

            while line:
                yield Event.from_dict(json.loads(line))

    def last(self, key: str) -> Event:
        # TODO: Fetch last from disk if it is not in cache.
        return self._cache.get(key)
