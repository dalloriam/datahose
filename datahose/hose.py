from collections import defaultdict

from datahose import stores as available_stores
from datahose.config import StoreConfiguration
from datahose.event import Event
from datahose.stores.store import MetricStore

from typing import Dict, List, Tuple


class Hose:

    def __init__(self, stores: List[StoreConfiguration]):
        m, s = Hose._initialize_stores(stores)
        self._store_map = m
        self._stores = s

        self.statistics: Dict[str, int] = defaultdict(lambda: 0)

    @staticmethod
    def _initialize_stores(stores: List[StoreConfiguration]) -> Tuple[Dict[str, List[MetricStore]], List[MetricStore]]:
        store_namespaces: Dict[str, List[MetricStore]] = defaultdict(lambda: [])
        ss: List[MetricStore] = []

        for store_cfg in stores:
            new_store = getattr(available_stores, store_cfg.name)(**store_cfg.config)
            ss.append(new_store)
            for n in store_cfg.namespaces:
                store_namespaces[n].append(new_store)

        return store_namespaces, ss

    def dispatch(self, e: Event) -> int:
        self.statistics[e.key] += 1

        i = 0
        for store in self._store_map.get(e.namespace, []):
            store.put(e)
            i += 1

        return i

    def flush(self):
        t = 0
        for s in self._stores:
            t += s.flush()
