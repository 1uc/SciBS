# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import scibs


class SequentialLocalBS(scibs.SciBS):
    """A batch system for when no batch system is present.

    Instead of submitting jobs to a batch system, those jobs will simply
    be run on the same computer.

    NOTE: This is blocking, i.e. `submit` only returns after the
          job has been completed.

    NOTE: By default, this will, in the simplest case, run
             subprocess.run(" ".join(job.cmd), shell=True, check=False)

          The important thing to observe is the `shell=True` part.
    """

    def __init__(self, submission_policy=None, wrap_policy=None):
        if submission_policy is None:
            submission_policy = scibs.SubprocessSubmissionPolicy(
                subprocess_kwargs={"check": False, "shell": True}
            )

        if wrap_policy is None:
            wrap_policy = scibs.DefaultWrapPolicy()

        self._submission_policy = submission_policy
        self._wrap_policy = wrap_policy

    def submit(self, job):
        cmd = self.cmdline(job)
        self._submission_policy(job.cwd, cmd)

    def cmdline(self, job):
        return self.wrap(job)

    def wrap(self, job):
        return self._wrap_policy(job)
