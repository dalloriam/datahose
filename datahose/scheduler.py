from datetime import datetime
from time import sleep

import asyncio


def _call_in_background(target, loop=None, executor=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    if callable(target):
        return loop.run_in_executor(executor, target)
    raise TypeError("target must be a callable, "
                    "not {!r}".format(type(target)))


class Scheduler(object):

    """
    The scheduler is used by the bot to schedule background tasks. By default,
    Aya uses it for emotion decay (emotion intensity decreases every hour).
    """

    # TODO: Change the scheduler so it is not a static class anymore

    subscriptions = []

    @classmethod
    def schedule_one(cls, task, time):
        """
        Schedule a one-off task.

        Args:
            task (callable): Task to schedule
            time (datetime): When to trigger the task
        """
        cls.subscriptions.append((task, time))
        cls.subscriptions.sort(key=lambda s: s[1])

    @classmethod
    def schedule_repeat(cls, task, delta):
        """
        Schedule a repeated task.

        Args:
            task (callable): Task to schedule
            delta (timedelta): Amount of time between task triggers
        """
        def repeat_task():
            task()

            next_time = datetime.now() + delta
            cls.schedule_one(repeat_task, next_time)

        cls.schedule_one(repeat_task, datetime.now())

    @classmethod
    def _run(cls):
        # TODO: Implement stop signal
        while True:
            if cls.subscriptions[0][1] < datetime.now():
                cb = cls.subscriptions[0][0]
                cls.subscriptions.pop(0)

                try:
                    cb()
                except Exception as e:
                    print(f'Error: {e}')
            sleep(1)

    @classmethod
    def start(cls):
        """
        Start the scheduler loop in background.
        """
        _call_in_background(cls._run)
