#!/usr/bin/env python3
"""
Clear orphaned Prefect flow runs so they release their deployment concurrency slot.

The problem this solves
-----------------------
Every deployment here has a concurrency limit with collision strategy CANCEL_NEW:
if a run already occupies the deployment's slot, the next scheduled run is
cancelled ("Deployment concurrency limit reached"). A slot is only released when
the occupying run reaches a TERMINAL state (Completed/Failed/Crashed/Cancelled).

On this single-worker, laptop-hosted setup a run frequently gets orphaned in a
NON-terminal state and never releases its slot:
  - the worker container is rebuilt/restarted mid-run (the flow subprocess dies
    with the worker, but the run is left PENDING/RUNNING on the server), or
  - the laptop sleeps and the run is left stranded, or
  - a Cancelling run never finishes cancelling.
Once an orphan holds the slot, EVERY future scheduled run of that deployment is
cancelled at 0s — silently, until the orphan is cleared by hand.

Why clearing on worker startup is safe here
--------------------------------------------
There is exactly one ProcessWorker, and it runs each flow as a child process.
When the worker starts, nothing it owns can still be executing — any flow run the
server still thinks is PENDING/RUNNING/PAUSED/CANCELLING is by definition an
orphan from a previous life. So marking every non-terminal run terminal at
startup is correct, and it frees all stuck slots before the worker begins
polling. SCHEDULED runs (future work) are left untouched.

Usage:
    python scripts/clear_orphan_runs.py                 # clear all orphans (startup)
    python scripts/clear_orphan_runs.py --min-age 120   # only orphans >120s old
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
from prefect.states import Crashed, Cancelled

# Non-terminal states that can hold a concurrency slot. SCHEDULED is deliberately
# excluded — those are legitimate future runs, not orphans.
ORPHAN_STATES = [StateType.PENDING, StateType.RUNNING,
                 StateType.PAUSED, StateType.CANCELLING]


async def clear(min_age_seconds: int = 0) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=min_age_seconds)
    cleared = 0
    async with get_client() as c:
        deps = {d.id: d.name for d in await c.read_deployments()}
        f = FlowRunFilter(state=FlowRunFilterState(
            type=FlowRunFilterStateType(any_=ORPHAN_STATES)))
        # The API caps limit at 200, so page through with an offset. We read the
        # full set before mutating any state, so offsets stay stable.
        runs, offset = [], 0
        while True:
            batch = await c.read_flow_runs(flow_run_filter=f, limit=200, offset=offset)
            runs.extend(batch)
            if len(batch) < 200:
                break
            offset += 200
        for r in runs:
            # Guard for manual mid-session use: skip very fresh runs so we don't
            # kill something the worker just legitimately picked up.
            ref = r.state.timestamp if r.state else None
            if min_age_seconds and ref and ref > cutoff:
                continue
            reason = "Cleared orphaned run on worker startup (no worker was executing it)"
            # A Cancelling run was already on its way out — finish the job.
            new_state = (Cancelled(message=reason)
                         if r.state and r.state.type == StateType.CANCELLING
                         else Crashed(message=reason))
            try:
                await c.set_flow_run_state(r.id, state=new_state, force=True)
                cleared += 1
                print(f"  cleared {deps.get(r.deployment_id,'?'):24} "
                      f"{r.name:20} {r.state.type.value} -> {new_state.type.value}")
            except Exception as e:  # noqa: BLE001 - best-effort cleanup
                print(f"  SKIP {r.name}: {type(e).__name__}: {str(e)[:80]}")
    print(f"orphan runs cleared: {cleared}")
    return cleared


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--min-age", type=int, default=0,
                   help="only clear orphans whose state is older than N seconds")
    asyncio.run(clear(p.parse_args().min_age))
