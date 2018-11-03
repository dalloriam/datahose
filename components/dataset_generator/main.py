from flask import Request, Response

from google.cloud import datastore

from http import HTTPStatus

from typing import List, Optional

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

    def update_datasets(self) -> None:
        """
        Updates all datasets with last added rows.
        """
        for kind in self.fetch_all_kinds():
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

            df = pd.DataFrame(hits) if existing_dataframe is None else existing_dataframe.append(pd.DataFrame(hits))
            print(f'Added {len(hits)} rows to dataset [{kind}].')

            # Write the appended dataset to file.
            with self.fs.open(self._get_kind_path(kind), 'w') as outfile:
                df.to_csv(outfile)


def generate_dataset(request: Request):
    project_name = os.environ.get('PROJECT_NAME')
    bucket_name = os.environ.get('BUCKET_NAME')

    try:
        updater = DatasetUpdater(project_name, bucket_name)
        updater.update_datasets()
    except Exception as e:
        return Response(
            response=json.dumps({'error': str(e)}),
            content_type='application/json'), HTTPStatus.INTERNAL_SERVER_ERROR

    return Response(response=json.dumps({'message': 'ok'}), content_type='application/json'), HTTPStatus.OK
