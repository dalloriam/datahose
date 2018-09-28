from abc import ABCMeta, abstractmethod

from datahose.event import Event

from typing import Iterator


class MetricStore(metaclass=ABCMeta):

    @abstractmethod
    def last(self, key: str) -> Event:
        """
        Returns the last inserted event.
        :param key: Desired event key.
        :return: The last event.
        """

    @abstractmethod
    def put(self, event: Event) -> None:
        """
        Inserts an event
        :param event: Event to insert.
        """

    @abstractmethod
    def __getitem__(self, key: str) -> Iterator[Event]:
        """
        Returns an iterator for the desired event type
        :param key: Event key.
        :return: Event Iterator
        """

    @abstractmethod
    def flush(self) -> None:
        """
        Write pending changes.
        """
