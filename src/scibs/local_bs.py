# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import os
import subprocess
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

    """

    def __init__(self, wrap_policy=None, resource_policy=None, local_resources=None):
        if wrap_policy is None:
            wrap_policy = scibs.DefaultWrapPolicy()

        if resource_policy is None:
            resource_policy = scibs.DefaultResourcePolicy()

        self._local_resources = local_resources
        self._wrap_policy = wrap_policy
        self._resource_policy = resource_policy
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

        job_schedule = scibs.GreedySchedule(
            self._context["jobs"], self._local_resources
        )
        _schedule_jobs(self._wrap_policy, self._resource_policy, job_schedule)


def _launch_job(wrap_policy, resource_policy, scheduled_job):
    job_id, job, acquired_resources = scheduled_job

    resource_policy(job, acquired_resources)
    cmd = wrap_policy(job)

    stdout = open(job.relative_to_cwd("cout"), "w")
    stderr = open(job.relative_to_cwd("cerr"), "w")

    proc = subprocess.Popen(
        cmd, cwd=job.cwd, stdout=stdout, stderr=stderr, env=job.env, shell=True
    )

    return proc, [stdout, stderr]


def _schedule_jobs(wrap_policy, resource_policy, job_schedule):
    pending = {}
    processes = {}

    def _wait():
        pid, _ = os.wait()
        assert pid in pending, "`os.wait` returned a PID that's not ours."

        job_id = pending.pop(pid)
        proc, files = processes.pop(pid)

        assert proc.pid == pid

        for f in files:
            f.close()

        job_schedule.complete(job_id)

    while not job_schedule.empty():
        next_job = job_schedule.next_job()

        assert (
            next_job or pending
        ), "It looks like there are no jobs pending and yet nothing can be submitted."

        if next_job is None:
            _wait()

        else:
            proc, files = _launch_job(wrap_policy, resource_policy, next_job)
            pid = proc.pid

            assert (
                pid not in processes
            ), "The OS reused a PID and we can't deal with it."

            pending[pid] = next_job[0]
            processes[pid] = (proc, files)

    while pending:
        _wait()
