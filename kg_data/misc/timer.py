import time
from contextlib import contextmanager


class TimerCount:
    def __init__(self, name: str, timer: 'Timer'):
        self.name = name
        self.timer = timer
        self.start = time.time()

    def stop(self):
        self.timer.categories[self.name] += time.time() - self.start
        return self.timer


class Timer:
    instance = None

    def __init__(self):
        self.categories = {}

    @staticmethod
    def get_instance():
        if Timer.instance is None:
            Timer.instance = Timer()
        return Timer.instance

    @contextmanager
    def watch(self, name: str = 'default'):
        try:
            count = self.start(name)
            yield None
        finally:
            count.stop()

    @contextmanager
    def watch_and_report(self, msg: str, print_fn=None):
        print_fn = print_fn or print
        try:
            start = time.time()
            yield None
        finally:
            end = time.time()
            print_fn(msg + f": {end - start:.3f} seconds")

    def start(self, name: str = 'default'):
        if name not in self.categories:
            self.categories[name] = 0.0
        return TimerCount(name, self)

    def report(self, print_fn=None):
        print_fn = print_fn or print
        if len(self.categories) == 0:
            print_fn("--- Nothing to report ---")
            return

        print_fn("Runtime report:")
        for k, v in self.categories.items():
            print_fn(f"\t{k}: {v:.3f} seconds")

    def get_time(self, name: str = 'default'):
        return self.categories[name]
