# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval
# Copyright (c) 2022 Luc Grosheintz-Laval

import scibs
from scibs import SciBS

class SLURM(SciBS):
    def __init__(self, submission_policy=None, wrap_policy=None):
        if submission_policy is None:
            submission_policy = scibs.MultiSubmissionPolicy(
                [scibs.StdOutSubmissionPolicy(), scibs.SubprocessSubmissionPolicy()]
            )

        if wrap_policy is None:
            wrap_policy = scibs.DefaultWrapPolicy()

        self._submission_policy = submission_policy
        self._wrap_policy = wrap_policy
        self._dependency_policy = scibs.SLURMDependencyPolicy()

    def submit(self, job, dependency=None):
        cmd = self.cmdline(job, dependency=dependency)
        self._submission_policy(cmd, cwd=job.cwd, env=job.env)

    def cmdline(self, job, dependency):
        r = job.resources
        c = [self.slurm_cmd]

        if dependency:
            c += [self._dependency_policy(dependency)]

        if job.name is not None:
            c += ["-J", job.name]

        if r.wall_clock is not None:
            c += ["--time", scibs.hhmmss(r.wall_clock)]

        if r.memory_per_core is not None:
            mem = r.memory_per_core * 1e-6
            c += [f"--mem-per-cpu={mem:.0f}"]

        if r.needs_mpi:
            c += [f"--ntasks={r.n_mpi_tasks}"]

        if r.needs_omp:
            c += [f"--cpus-per-task={r.n_omp_threads}"]

        if not r.needs_mpi and not r.needs_omp:
            c += [f"--cpus-per-task={r.n_cores}"]

        if r.needs_gpus:
            raise NotImplementedError("Need to consider GPUs.")

        c += self.site_specific_flags(job)
        c += self.wrap(job)

        return c

    def wrap(self, job):
        return [self._wrap_policy(job)]

    def site_specific_flags(self, job):
        return []

    @property
    def slurm_cmd(self):
        raise NotImplementedError("This needs to be implemented first.")
        # return "srun"


class SBatchMixin:
    """A queue for submitting sbatch scripts.
    
    This mixin modifies a queue/batch system differ to expect sbatch scripts,
    and only works for those. If you want to submit regular commands, use
    `SLURM`.
    """
    def __init__(self, *args, wrap_policy=None, **kwargs):
        if wrap_policy is None:
            wrap_policy = scibs.SBatchWrapPolicy()

        super().__init__(*args, wrap_policy=wrap_policy, **kwargs)

    @property
    def slurm_cmd(self):
        return "sbatch"

    def wrap(self, job):
        return self._wrap_policy(job)


class BB5(SLURM):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SBatchBB5(SBatchMixin, BB5):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
