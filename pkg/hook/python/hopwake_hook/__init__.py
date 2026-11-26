"""Hopwake hook for Python services: lets a service say that it stored or
served a reading, for hops no tap can watch from outside.

    from hopwake_hook import Hook, STORE, SERVE

    with Hook(tenant="reference", hop="hook:api.served") as hook:
        hook.mark(SERVE, "dev-00042", 1683)

Marks are sent as OTLP/JSON spans to Hopwake's collector, keyed by the same
journey ID a tap derives. The contract is in docs/hooks.md.
"""

from .hook import SERVE, STORE, Hook
from .journey import format_alias, format_time, journey_id, parse_duration, time_journey_id, unix_nanos

__version__ = "0.1.0"

__all__ = [
    "Hook",
    "STORE",
    "SERVE",
    "journey_id",
    "time_journey_id",
    "format_time",
    "format_alias",
    "parse_duration",
    "unix_nanos",
]
