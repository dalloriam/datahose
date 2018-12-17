from dalloriam.datahose import DatahoseClient

from flask import Request, Response

from google.cloud import datastore

from http import HTTPStatus

from typing import Dict, List, Optional

import gcsfs
import json
import os
import pandas as pd


class DatasetUpdater:

    _SCROLL_BUF_SIZE = 50

    def __init__(self, project_name: str, bucket_name: str) -> None:
        self.fs = gcsfs.GCSFileSystem(project=project_name)
        self.ds = datastore.Client()

        self._bucket_name = bucket_name

    def _get_kind_path(self, kind: str) -> str:
        return f'{self._bucket_name}/{kind}'

    def _try_load_dataset(self, kind: str) -> Optional[pd.DataFrame]:
        fname = self._get_kind_path(kind)

        if not self.fs.exists(fname):
            return None

        # Read pandas dataframe from bucket
        with self.fs.open(fname, 'r') as infile:
            return pd.read_csv(infile)

    def fetch_all_kinds(self) -> List[str]:
        """
        Fetches all public event kinds from the default datastore namespace.
        Returns:
            List of keys.
        """
        query = self.ds.query(kind='__kind__')
        return [x.key.name for x in query.fetch() if not x.key.name.startswith('_')]

    def update_dataset(self, kind: str) -> int:
        """
        Updates a single dataset for a given kind.
        Args:
            kind (str): Datastore kind.

        Returns:
            The number of new rows since the last update.
        """
        existing_dataframe = self._try_load_dataset(kind)

        query = self.ds.query(kind=kind)

        if existing_dataframe is not None and 'time' in existing_dataframe:
            query.add_filter('time', '>', existing_dataframe['time'].max())

        offset = 0
        hits = []
        while True:
            out = list(query.fetch(limit=DatasetUpdater._SCROLL_BUF_SIZE, offset=offset))
            hits += out
            if len(out) < DatasetUpdater._SCROLL_BUF_SIZE:
                break
            offset += len(out)

        cleaned_hits = [{k: v for k, v in hit.items() if k} for hit in hits]
        if existing_dataframe is None:
            df = pd.DataFrame(cleaned_hits)
        else:
            df = existing_dataframe.append(pd.DataFrame(cleaned_hits), sort=True)

        print(f'Added {len(hits)} rows to dataset [{kind}].')

        # Write the appended dataset to file.
        with self.fs.open(self._get_kind_path(kind), 'w') as outfile:
            df.to_csv(outfile, index=False)

        return len(hits)

    def update_datasets(self) -> Dict[str, int]:
        """
        Updates all datasets with last added rows.

        Returns:
            The dictionary of updated items.
        """
        return {
            kind: self.update_dataset(kind)
            for kind in self.fetch_all_kinds()
        }


def dispatch_update_results(update_results: Dict[str, int]) -> None:
    stats_lst = [f'  - `{event_key}`: {val}' for event_key, val in update_results.items() if val != 0]

    if not stats_lst:
        return

    datahose = DatahoseClient(
        service_host=os.environ.get('DATAHOSE_HOST'),
        password=os.environ.get('DATAHOSE_PASSWORD')
    )
    datahose.notify(
        sender="Event Report",
        message='Since last report: \n\n' + '\n'.join(stats_lst)
    )


def generate_dataset(event, context):
    project_name = os.environ.get('PROJECT_NAME')
    bucket_name = os.environ.get('BUCKET_NAME')

    try:
        updater = DatasetUpdater(project_name, bucket_name)
        update_results = updater.update_datasets()
        dispatch_update_results(update_results)
    except Exception as e:
        return Response(
            response=json.dumps({'error': str(e)}),
            content_type='application/json'), HTTPStatus.INTERNAL_SERVER_ERROR

    return Response(response=json.dumps({'message': 'ok'}), content_type='application/json'), HTTPStatus.OK
