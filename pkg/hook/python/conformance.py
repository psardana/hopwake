"""Runs the hook conformance script (docs/hooks.md) with the Python hook.
Run it under hookcheck:

    go run ./cmd/hookcheck -service conformance-python -- python3 pkg/hook/python/conformance.py
"""

import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hopwake_hook import SERVE, STORE, Hook  # noqa: E402


def fail(msg: str) -> None:
    print("conformance: " + msg, file=sys.stderr)
    sys.exit(1)


def main() -> None:
    try:
        h = Hook(tenant=os.environ.get("HOPWAKE_TENANT"), hop=os.environ.get("HOPWAKE_HOP"), service="conformance-python")
    except ValueError as e:
        fail(str(e))
    at = dt.datetime(2026, 11, 18, 10, 0, 0, 750000, tzinfo=dt.timezone.utc)
    typ = "ems.measurements.Measurement"
    h.mark(STORE, "dev-00042", 1683)
    h.mark_boot(STORE, "dev-00042", "7", 12)
    h.mark(SERVE, "dev-00042", 1683)
    h.mark(SERVE, "dev-00042", 1683)  # the same reading served again: not sent
    h.mark_time(STORE, "EMS0000042", typ, at)
    h.mark_time_teach(STORE, "EMS0000042", "systemId", "100042", typ, at)
    if h.accepted != 5:
        fail("%d marks queued after step 6, want 5" % h.accepted)
    # The alias list arrives shortly after the hook starts.
    deadline = time.monotonic() + 15
    while not h.mark_time_alias(SERVE, "systemId", "100042", typ, at):
        if time.monotonic() > deadline:
            fail("the alias systemId:100042 was never known")
        time.sleep(0.1)
    before = h.accepted
    if h.mark_time_alias(SERVE, "systemId", "999999", typ, at):
        fail("the unknown alias systemId:999999 was resolved")
    if h.accepted != before:
        fail("the unknown alias systemId:999999 queued a mark")
    if not h.close():
        fail("close did not send everything in time")
    if h.dropped:
        fail("%d marks dropped" % h.dropped)


if __name__ == "__main__":
    main()
