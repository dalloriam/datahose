from collections import namedtuple, defaultdict

from components.report_generator.main import get_statistics

from typing import Any, List, Tuple

from unittest import mock


Statistic = namedtuple('Statistic', ['user_id', 'key', 'count'])


def iterator(x):
    yield from x


class MockClient:

    def __init__(self, hits: List[Tuple[Any, ...]]):
        self._hits = hits

    def query(self, *args, **kwargs):
        yield from self._hits


def test_get_statistics():
    mock_rows = [
        Statistic('user_a', "personal.test", 12),
        Statistic('user_a', "personal.other", 13),
        Statistic('user_b', "personal.test", 14),
        Statistic('user_b', "personal.other", 15),
    ]

    mock_client = MockClient(mock_rows)

    with mock.patch('google.cloud.bigquery.Client', return_value=mock_client):
        stats = get_statistics()

        assert stats == defaultdict(dict, {
            'user_a': {
                'personal.test': 12,
                'personal.other': 13
            },
            'user_b': {
                'personal.test': 14,
                'personal.other': 15
            }
        })
