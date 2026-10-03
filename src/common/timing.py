"""Wall-clock timings for pipeline steps, recorded in run summaries."""

from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter


class Timings(dict):
    """Step name → seconds, e.g. {"extract": 0.41, "warehouse": 3.2}."""

    @contextmanager
    def measure(self, step: str) -> Iterator[None]:
        """Record how long the block takes as `step` (even if it raises)."""

        started = perf_counter()

        try:
            yield
        finally:
            self[step] = round(perf_counter() - started, 3)

    def describe(self) -> str:
        """Human-readable, e.g. 'extract 0.41s, warehouse 3.20s'."""

        return ", ".join(
            f"{step} {seconds:.2f}s"
            for step, seconds in self.items()
        )
