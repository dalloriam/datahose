from dalloriam.api import API, authenticated

from datahose.config import StoreConfiguration
from datahose.event import EventSchema, Event
from datahose.hose import Hose
from datahose.scheduler import Scheduler

from typing import List

from webargs.flaskparser import use_args

import os
import json


SETTINGS_FILE_ENV = 'DATA_SETTINGS_FILE'
SECRET_ENV = 'DATA_APP_SECRET'


def _get_password() -> str:
    return os.environ.get(SECRET_ENV, 'secret')


def _init_config() -> List[StoreConfiguration]:
    settings_file = os.environ.get(SETTINGS_FILE_ENV, '/config.json')
    with open(settings_file, 'r') as infile:
        config = json.load(infile)

    store_configs: List[StoreConfiguration] = []
    for cfg in config['stores']:
        store_configs.append(StoreConfiguration(**cfg))

    return store_configs


app = API(host='0.0.0.0', port=8080, debug=False)
svc = Hose(_init_config())
Scheduler.start()


@app.route('/event', methods=['POST'])
@use_args(EventSchema(strict=True))
@authenticated(_get_password())
def receive_event(evt_data: dict):
    evt = Event(**evt_data)
    svc.dispatch(evt)
    return evt.serialized


@app.route('/flush', methods=['POST'])
@authenticated(_get_password())
def flush_all():
    svc.flush()
    return {'flushed': True}


def main():
    # TODO: Support proper config
    app.start()
