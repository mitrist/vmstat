"""Агенты пайплайна Диспетчер → Разработчик → QA (см. agents.md и скилл pipeline)."""

from .dispatcher import DispatcherAgent, Verdict

__all__ = ["DispatcherAgent", "Verdict"]
