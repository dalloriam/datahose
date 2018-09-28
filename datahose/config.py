from dataclasses import dataclass

from typing import List


@dataclass
class StoreConfiguration:
    name: str
    namespaces: List[str]
    config: dict
