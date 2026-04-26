"""
Агент Диспетчер: план (список строк) и индекс текущей задачи.
"""

from __future__ import annotations

from typing import Literal

Verdict = Literal["OK", "FAIL"]


class DispatcherAgent:
    """Управляет порядком пунктов плана; реагирует на вердикт QA."""

    def __init__(self, plan: list[str]) -> None:
        self._plan: list[str] = [p.strip() for p in plan if p and str(p).strip()]
        self._index: int = 0

    @property
    def current_index(self) -> int:
        return self._index

    @property
    def total(self) -> int:
        return len(self._plan)

    def getNextTask(self) -> str:
        """Следующий пункт плана или ALL_DONE."""
        if self._index >= len(self._plan):
            return "ALL_DONE"
        return self._plan[self._index]

    def receiveQAVerdict(self, verdict: Verdict) -> None:
        """OK — сдвинуть индекс; FAIL — оставить тот же пункт."""
        if verdict == "OK":
            self._index += 1

    def reset(self, plan: list[str] | None = None) -> None:
        self._index = 0
        if plan is not None:
            self._plan = [p.strip() for p in plan if p and str(p).strip()]
