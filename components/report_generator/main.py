from collections import defaultdict

from google.cloud import bigquery, error_reporting, pubsub_v1

from typing import Dict

import json
import os


_SCROLL_BUF_SIZE = 50


def get_statistics() -> Dict[str, Dict[str, int]]:
    bq = bigquery.Client()
    query_job = bq.query("SELECT user_id, key, COUNT(*) as count FROM datahose.events "
                         "WHERE time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 24 HOUR) GROUP BY user_id, key;")

    statistics = defaultdict(dict)

    for row in query_job:
        statistics[row.user_id][row.key] = row.count

    return statistics


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


def send_reports(event, context):
    client = error_reporting.Client()

    try:
        statistics = get_statistics()
        for user_id, user_stats in statistics.items():
            dispatch_update_results(user_id, user_stats)
    except Exception:
        client.report_exception()

    print('Reports sent successfully')
