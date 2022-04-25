from collections import Counter
from typing import List


class StatsRecorder:
    def __init__(self) -> None:
        self.counter = Counter()

    def inc_stat(self, key: str, amount=1):
        self.counter[key] += amount

    def get_stats(self, keys: List[str] = None):
        if keys is None:
            return self.counter.copy()
        output: dict = {}
        for key in keys:
            output[key] = self.counter[key]
        return output
