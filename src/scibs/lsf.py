# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import scibs
from scibs import SciBS


class LSF(SciBS):
    def __init__(self, submission_policy=None, wrap_policy=None):
        if submission_policy is None:
            submission_policy = scibs.MultiSubmissionPolicy(
                [scibs.StdOutSubmissionPolicy(), scibs.SubprocessSubmissionPolicy()]
            )

        if wrap_policy is None:
            wrap_policy = scibs.DefaultWrapPolicy()

        self._submission_policy = submission_policy
        self._wrap_policy = wrap_policy

    def submit(self, job):
        cmd = self.cmdline(job)
        self._submission_policy(cmd, cwd=job.cwd, env=job.env)

    def cmdline(self, job):
        r = job.resources
        c = ["bsub"]

        if job.name is not None:
            c += ["-J", job.name]

        if r.wall_clock is not None:
            c += ["-W", scibs.hhmm(r.wall_clock)]

        if r.memory_per_core is not None:
            mem = r.memory_per_core * 1e-6
            c += ["-R", f"rusage[mem={mem:.0f}]"]

        assert r.n_cores is not None, "The total number of cores is mandatory."
        assert r.n_cores >= 1, "Total number of cores must be at least 1."

        c += ["-n", str(r.n_cores)]

        if r.needs_gpus:
            c += ["-R", f"rusage[ngpus_excl_p={r.n_gpus_per_process}]"]

        c += self.site_specific_flags(job)
        c += self.wrap(job)

        return c

    def wrap(self, job):
        return [self._wrap_policy(job)]

    def site_specific_flags(self, job):
        return []


class EulerLSF(LSF):
    """The ETH cluster Euler uses LSF."""

    def __init__(self, submission_policy=None, wrap_policy=None):
        if wrap_policy is None:
            wrap_policy = scibs.EulerWrapPolicy()

        super().__init__(submission_policy=submission_policy, wrap_policy=wrap_policy)

    def site_specific_flags(self, job):
        # The part I want to target has 2x64 cores.
        ptile = min(128, job.resources.n_cores)
        return ["-R", f"span[ptile={ptile}]"]
