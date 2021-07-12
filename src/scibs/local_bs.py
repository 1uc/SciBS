# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import asyncio
import datetime

import scibs


class LocalBS(scibs.SciBS):
    """An ad hoc local batch system for when no batch system is present.

    Instead of submitting jobs to a batch system, those jobs will simply
    be run on the same computer. If there are sufficient resources available,
    multiple jobs will be run in parallel.

    NOTE: This will, in the simplest case, run
             subprocess.run(" ".join(job.cmd), shell=True, check=False)

          The important thing to observe is the `shell=True` part.

    NOTE: This will call `asyncio.run`, starting an event loop. This is not
          a problem unless you're already using `asyncio`.
    """

    def __init__(self, wrap_policy=None):
        if wrap_policy is None:
            wrap_policy = scibs.DefaultWrapPolicy()

        self._wrap_policy = wrap_policy
        self._context = None

    def __enter__(self):
        self._context = {"jobs": []}
        return self

    def __exit__(self, *args):
        self._run_all()
        self._context = None

    def submit(self, job):
        self._ensure_with_context()
        self._context["jobs"].append(job)

    def _ensure_with_context(self):
        if self._context is None:
            # This implementation must use a `with` statement to
            # properly acquire the resources it needs. Ensure it's being used
            # as follows
            #    with LocalBS() as local_bs:
            #        local_bs.submit(...)
            #        ...
            #        local_bs.submit(...)
            raise RuntimeError(f"{__class__} is missing context.")

    def _run_all(self):
        self._ensure_with_context()

        job_schedule = scibs.GreedySchedule(self._context["jobs"])
        asyncio.run(_schedule_jobs(self._wrap_policy, job_schedule))


async def _launch_job(wrap_policy, job_id, job):
    cmd = wrap_policy(job)
    proc = await asyncio.create_subprocess_shell(cmd, cwd=job.cwd, env=job.env)
    await proc.wait()
    return {"job_id": job_id}


async def _schedule_jobs(wrap_policy, job_schedule):
    pending = []
    while not job_schedule.empty():
        next_job = job_schedule.next_job()

        assert (
            next_job or pending
        ), "It looks like there are no jobs pending and yet nothing can be submitted."

        if next_job is None:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            pending = list(pending)

            for task in done:
                result = task.result()
                job_schedule.complete(result["job_id"])

        else:
            job_id, job = next_job
            pending.append(asyncio.create_task(_launch_job(wrap_policy, job_id, job)))

    _, pending = await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
    assert len(pending) == 0
