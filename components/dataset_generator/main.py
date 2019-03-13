from dateutil import parser

from google.cloud import datastore, error_reporting, pubsub_v1

from typing import Any, Dict, List, Optional

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

    def _get_kind_path(self, user_id: str, kind: str) -> str:
        return f'{self._bucket_name}/{user_id}/{kind}'

    def _try_load_dataset(self, user_id: str, kind: str) -> Optional[pd.DataFrame]:
        fname = self._get_kind_path(user_id, kind)

        if not self.fs.exists(fname):
            return None

        # Read pandas dataframe from bucket
        with self.fs.open(fname, 'r') as infile:
            return pd.read_csv(infile)

    def list_user_ids(self) -> List[str]:
        """
        Lists all user IDs.
        Returns:
            A list of all user IDs.
        """
        query = self.ds.query(kind='User')
        hits = []
        offset = 0
        while True:
            out = list(query.fetch(limit=DatasetUpdater._SCROLL_BUF_SIZE, offset=offset))
            hits += out
            if len(out) < DatasetUpdater._SCROLL_BUF_SIZE:
                break
            offset += len(out)

        return [x.key.name for x in hits]

    def fetch_all_kinds(self) -> List[str]:
        """
        Fetches all public event kinds from the default datastore namespace.
        Returns:
            List of keys.
        """
        query = self.ds.query(kind='__kind__')
        return [x.key.name for x in query.fetch() if x.key.name.startswith('Event')]

    def _fetch_hits(self, user_id: str, kind: str, time_since_last_event: Optional[float]) -> List[Dict[str, Any]]:
        print(kind)
        query = self.ds.query(kind=kind)

        query.add_filter('dh_user_id', '=', user_id)

        if time_since_last_event is not None:
            query.add_filter('time', '>', parser.parse(time_since_last_event))

        offset = 0
        hits = []
        while True:
            out = list(query.fetch(limit=DatasetUpdater._SCROLL_BUF_SIZE, offset=offset))
            hits += out
            if len(out) < DatasetUpdater._SCROLL_BUF_SIZE:
                break
            offset += len(out)

        return [{k: v for k, v in hit.items() if k} for hit in hits]

    def update_dataset(self, user_id: str, kind: str) -> int:
        """
        Updates a single dataset for a given kind.
        Args:
            user_id (str): The ID of the user for which to update the datasets.
            kind (str): Datastore kind.

        Returns:
            The number of new rows since the last update.
        """
        existing_dataframe = self._try_load_dataset(user_id, kind)

        cleaned_hits = self._fetch_hits(
            user_id, kind,
            existing_dataframe['time'].max()
            if existing_dataframe is not None and 'time' in existing_dataframe else None)

        if existing_dataframe is None:
            df = pd.DataFrame(cleaned_hits)
        else:
            df = existing_dataframe.append(pd.DataFrame(cleaned_hits), sort=True)

        print(f'Added {len(cleaned_hits)} rows to dataset [{kind}].')

        # Write the appended dataset to file.
        if len(cleaned_hits) > 0:
            with self.fs.open(self._get_kind_path(user_id, kind), 'w') as outfile:
                df.to_csv(outfile, index=False)

        return len(cleaned_hits)

    def update_datasets(self, user_id: str) -> Dict[str, int]:
        """
        Updates all datasets with last added rows.

        Args:
            user_id (str): The ID of the user.

        Returns:
            The dictionary of updated items.
        """
        return {
            kind: self.update_dataset(user_id, kind)
            for kind in self.fetch_all_kinds()
        }


def dispatch_update_results(user_id: str, update_results: Dict[str, int]) -> None:
    stats_lst = [f'  - `{event_key}`: {val}' for event_key, val in update_results.items() if val != 0]

    if not stats_lst:
        return

    publisher: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient()

    project_name = os.environ.get('PROJECT_NAME')
    topic_path = publisher.topic_path(project_name, 'notifications')
    publisher.publish(topic_path, data=json.dumps({  # TODO: Find a way to fetch topic dynamically
        'key': 'notification.reports',
        'user_id': user_id,
        'body': {
            'sender': 'Event Report',
            'message': 'Since last report: \n\n' + '\n'.join(stats_lst)
        }
    }).encode())


def generate_dataset(event, context):
    project_name = os.environ.get('PROJECT_NAME')
    bucket_name = os.environ.get('DATASET_BUCKET_NAME')

    client = error_reporting.Client()

    try:
        updater = DatasetUpdater(project_name, bucket_name)

        for user_id in updater.list_user_ids():
            update_results = updater.update_datasets(user_id)
            dispatch_update_results(update_results)
    except Exception:
        client.report_exception()

    print('Dataset update completed successfully')
