from collections import defaultdict

from datahose.event import Event

from google.cloud import pubsub_v1

from typing import Dict, List


class Hose:

    def __init__(self, project_name: str, namespace_topics: Dict[str, List[str]]):
        self._publisher: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient()
        self._namespace_topics = {
            k: [
                self._publisher.topic_path(project_name, v)
                for v in t
            ]
            for k, t in namespace_topics.items()
        }

        self.statistics: Dict[str, int] = defaultdict(lambda: 0)

    def dispatch(self, e: Event) -> int:
        self.statistics[e.key] += 1

        i = 0
        for topic_path in self._namespace_topics.get(e.namespace, []):
            print(f'Publishing event [{e.key}] to [{topic_path}]...')
            self._publisher.publish(topic_path, data=e.serialized)

        return i
