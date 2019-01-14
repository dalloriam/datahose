from components.dataset_generator.main import DatasetUpdater

from dateutil import parser

from tests.utils.query_mock import QueryMock, ObjectMock

from typing import Any, Dict, List, Optional

from unittest import mock

import pandas as pd
import pytest


@pytest.mark.parametrize(
    'bucket_name,kind,expected_path',
    [
        ('some_bucket', 'some_kind', 'some_bucket/some_kind'),
        ('some_bucket', '', 'some_bucket/'),
        ('', 'some_kind', '/some_kind'),
        ('', '', '/')
    ]
)
def test_dataset_updater_get_kind_path_returns_expected_path(bucket_name: str, kind: str, expected_path: str) -> None:
    with mock.patch('gcsfs.GCSFileSystem'), mock.patch('google.cloud.datastore.Client'):
        d = DatasetUpdater('project-name', bucket_name=bucket_name)
        assert expected_path == d._get_kind_path(kind)


@pytest.mark.parametrize(
    'path_exists,expected_dataset',
    [
        (False, None),
        (True, pd.DataFrame(['hello', 'world']))
    ]
)
def test_dataset_updater_try_load_dataset(path_exists: bool, expected_dataset: Optional[pd.DataFrame]) -> None:
    with mock.patch('gcsfs.GCSFileSystem'), mock.patch('google.cloud.datastore.Client'):
        d = DatasetUpdater('project-name', bucket_name='someBUCKET')
        with mock.patch.object(d.fs, 'exists', return_value=path_exists), \
                mock.patch('pandas.read_csv', return_value=expected_dataset):
            if expected_dataset is None:
                assert d._try_load_dataset('kind') is None
            else:
                assert expected_dataset.equals(d._try_load_dataset('kind'))


@pytest.mark.parametrize(
    'query_keys,expected_kinds',
    [
        ([], []),
        (['_hello'], []),
        (['a', '_hello', 'b'], ['a', 'b'])
    ]
)
def test_dataset_updater_fetch_all_kinds(query_keys: List[str], expected_kinds: List[str]):
    with mock.patch('gcsfs.GCSFileSystem'), mock.patch('google.cloud.datastore.Client'):
        d = DatasetUpdater('some-project', 'some-bucket')
        with mock.patch.object(d.ds, 'query', return_value=QueryMock(objects=[{'key': k} for k in query_keys])):
            assert expected_kinds == d.fetch_all_kinds()


@pytest.mark.parametrize(
    'objects,max_time,max_fetch,expected_hits',
    [
        # case with no hits.
        ([], None, None, []),

        # simple case with no data
        ([{'key': 'testitem'}], None, None, [{}]),

        # simple case with data
        ([{'key': 'testitem', 'hello': 'world'}], None, None, [{'hello': 'world'}]),

        # case with paging
        ([{'key': 'item1', 'a': 'b'}, {'key': 'item2', 'c': 'd'}], None, 1, [{'a': 'b'}, {'c': 'd'}]),

        # case with filter
        (
            [
                {'key': 'item1', 'time': parser.parse('2018-12-16 22:11:53.253675+00:00')},
                {'key': 'item2', 'time': parser.parse('2018-12-16 22:12:53.253675+00:00')},
                {'key': 'item3', 'time': parser.parse('2018-12-16 22:13:53.253675+00:00')}
            ],
            '2018-12-16 22:11:53.253675+00:00',
            None,
            [
                {'time': parser.parse('2018-12-16 22:12:53.253675+00:00')},
                {'time': parser.parse('2018-12-16 22:13:53.253675+00:00')}
            ]
        )
    ]
)
def test_dataset_updater_fetch_hits(
        objects: List[ObjectMock],
        max_time: Optional[float],
        max_fetch: Optional[int],
        expected_hits: List[Dict[str, Any]]) -> None:

    with mock.patch('gcsfs.GCSFileSystem'), mock.patch('google.cloud.datastore.Client'):
        d = DatasetUpdater('some-project', 'some-bucket')

        if max_fetch is not None:
            old_fetch = DatasetUpdater._SCROLL_BUF_SIZE
            DatasetUpdater._SCROLL_BUF_SIZE = max_fetch

        with mock.patch.object(d.ds, 'query', return_value=QueryMock(objects=objects)):
            actual_hits = d._fetch_hits('some_kind', max_time)
            assert expected_hits == actual_hits

        if max_fetch is not None:
            DatasetUpdater._SCROLL_BUF_SIZE = old_fetch
