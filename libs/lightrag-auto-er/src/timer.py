import time
from typing import Dict

class Timer:
    def __init__(self):
        self.timings: Dict[str, float] = {}
        self._starts: Dict[str, float] = {}

    def start(self, key: str):
        self._starts[key] = time.perf_counter()

    def stop(self, key: str) -> float:
        if key not in self._starts:
            return 0.0
        elapsed = time.perf_counter() - self._starts[key]
        self.timings[key] = elapsed
        return elapsed

    def get(self, key: str) -> float:
        return self.timings.get(key, 0.0)

    def get_all(self) -> Dict[str, float]:
        return self.timings

# Global timer instance
timer = Timer()
