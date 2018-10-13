from dalloriam.api import API
from dalloriam.notification import NotifierClient

from datahose.event import EventSchema, Event
from datahose.hose import Hose
from datahose.scheduler import Scheduler

from datetime import timedelta

from typing import Dict, List, Tuple

from webargs.flaskparser import use_args

import os
import json


SETTINGS_FILE_ENV = 'DATA_SETTINGS_FILE'
SECRET_ENV = 'DATA_APP_SECRET'

NOTIFY_ENV = 'NOTIF_PASSWORD'


def _get_password() -> str:
    return os.environ.get(SECRET_ENV, 'secret')


def _get_notif_coords() -> Tuple[str, str]:
    return (
        os.environ.get('NOTIF_HOST', 'http://service.williamdussault.com/notify'),
        os.environ.get('NOTIF_PASS', 'secret')
    )


def _init_config() -> Dict[str, List[str]]:
    settings_file = os.environ.get(SETTINGS_FILE_ENV, '/config.json')
    with open(settings_file, 'r') as infile:
        config = json.load(infile)

    return config


app = API(host='0.0.0.0', port=8080, debug=False)
svc = Hose(os.environ.get('GCLOUD_PROJECT_NAME', 'personal-workspace'), _init_config())


def post_statistics_digest() -> None:
    print('Sending digest')
    host, password = _get_notif_coords()
    client = NotifierClient(host, password)

    stats = svc.statistics
    if not stats:
        return

    out_str = 'Since Last Update\n' + '\n'.join([f'  - `{key}`: {count}' for key, count in stats.items()])
    client.send('Event Digest', out_str)
    svc.statistics.clear()


@app.route('/event', methods=['POST'])
@use_args(EventSchema(strict=True))
def receive_event(evt_data: dict):
    evt = Event(**evt_data)
    svc.dispatch(evt)
    return evt.dict


def main():
    # TODO: Support proper config
    Scheduler.schedule_repeat(post_statistics_digest, timedelta(hours=12))
    Scheduler.start()
    app.start()
