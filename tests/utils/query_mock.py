from typing import Any, Dict, Iterator, List, Tuple

import json
import numbers


class KeyMock:

    def __init__(self, name: str):
        self.name = name


class ObjectMock:

    def __init__(self, key: str, data: Dict[str, Any] = None):
        self.key = KeyMock(key)
        self._data = data

    def __contains__(self, item: str) -> bool:
        return item in self._data

    def __getitem__(self, item: str) -> Any:
        return self._data[item]

    def __str__(self) -> str:
        return json.dumps(self._data)  # pragma: nocover

    def __repr__(self) -> str:
        return str(self)  # pragma: nocover

    def items(self) -> Iterator[Tuple[str, Any]]:
        yield from self._data.items()


class QueryMock:

    def __init__(self, objects: List[Dict[str, Any]]):
        self._vals = [ObjectMock(x['key'], {k: v for k, v in x.items() if k != 'key'}) for x in objects]
        self._filters = []

    def add_filter(self, field_name: str, op: str, value: numbers.Real) -> None:
        self._filters.append((field_name, op, value))

    def fetch(self, offset: int = 0, limit: int = 0) -> Iterator[KeyMock]:
        hits = []

        for obj in self._vals:
            failed_one_filter = False

            for (obj_key, operator, value) in self._filters:
                if obj_key not in obj:
                    break  # pragma: nocover

                if operator == '>' and obj[obj_key] <= value:
                    failed_one_filter = True

                if operator == '<' and obj[obj_key] >= value:
                    failed_one_filter = True  # pragma: nocover

            if not failed_one_filter:
                hits.append(obj)

        count = 0
        for obj in hits[offset:]:
            count += 1
            if limit != 0 and count > limit:
                break

            yield obj
