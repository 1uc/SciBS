# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import psutil
import os
import datetime


class Schedule:
    def empty(self):
        """Has everything been scheduled?"""
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `empty`."
        )

    def next_job(self):
        """Returns the tuple `(job_id, job)` for the next job to be run.

        It is possible that there are not enough resources to run a further
        job, in this case `None` is returned instead.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `next_job`."
        )

    def complete(self, job_id):
        """Indicate to the `Schedule` that the job with `job_id` has completed."""
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `complete`."
        )


class GreedySchedule(Schedule):
    """The most simple, i.e. greedy, scheduling possible."""

    def __init__(self, jobs, local_resources=None):
        if local_resources is None:
            local_resources = LocalResources()

        self._local_resources = local_resources
        self._jobs = sorted(jobs, key=self._job_order)
        self._job_info = [
            {"id": k, "complete": False, "scheduled": False}
            for k, _ in enumerate(self._jobs)
        ]

        self._unscheduled_jobs = list(range(len(self._jobs)))

    def empty(self):
        return self._unscheduled_jobs == []

    def next_job(self):
        def acquire(job_id):
            r = self._jobs[job_id].resources
            acquired_resources = self._local_resources.acquire(r)

            if acquired_resources is not None:
                self._job_info[job_id]["resources"] = acquired_resources
                self._job_info[job_id]["scheduled"] = True
                del self._unscheduled_jobs[self._unscheduled_jobs.index(job_id)]

            return acquired_resources

        try:
            job_id = next(
                iter(
                    job_id
                    for job_id in self._unscheduled_jobs
                    if acquire(job_id) is not None
                )
            )

        except StopIteration:
            return None

        job = self._jobs[job_id]
        allocated_resources = self._job_info[job_id]["resources"]
        return job_id, job, allocated_resources

    def complete(self, job_id):
        self._job_info[job_id]["complete"] = True
        self._local_resources.release(self._job_info[job_id]["resources"])

    def _job_order(self, job):
        # Longer jobs have high priority over shorter jobs. Ties are broken by
        # the number of resources used. Finally, no wall-clock requirements
        # indicates fast jobs, since otherwise these jobs would need to ask
        # for a wall-clock allowance in a real batch system.

        r = job.resources
        wall_clock = r.wall_clock if r.wall_clock else datetime.timedelta(seconds=0)
        n_cores = r.n_cores
        return (-wall_clock, -n_cores)


class LocalResources:
    """Models the currently available resources.

    Note: The interface is unlikely to be stable, since it is not suited for
          anything more efficient than linear searches to find eligible jobs.
    """

    def __init__(self, cores=None):
        if cores is None:
            cores = psutil.cpu_count()

        self._cores = cores

    def acquire(self, resources):
        """Try to acquire the requested resources.

        Returns either the acquired resources or `None` if the request can't be
        satisfied.

        Args:
            resources: A `scibs.Resource` representing the required
                       resources to run the job.
        """

        requested_cores = resources.n_cores

        if self._cores >= requested_cores:
            self._cores -= requested_cores
            return {"cores": requested_cores}

        else:
            return None

    def release(self, acquired_resources):
        """Return the acquired resources once they are no longer used."""
        self._cores += acquired_resources["cores"]
        acquired_resources.pop("cores", None)


class LocalGPUResources:
    def __init__(self, available_gpus=None):
        """Create the available GPU resources.

        Arguments:
            available_gpus  Either a list of integers, or a comma separated
                            string, e.g., the environment variable
                            CUDA_VISIBLE_DEVICE.
        """

        if available_gpus is None:
            available_gpus = os.environ["CUDA_VISIBLE_DEVICES"]

        if isinstance(available_gpus, str):
            available_gpus = available_gpus.split(",")
            available_gpus = [int(gpu_id) for gpu_id in available_gpus]

        self._gpus = available_gpus

    def acquire(self, resources):
        assert resources.needs_gpus

        n_requested_gpus = resources.n_gpus_per_process
        assert n_requested_gpus > 0

        # Deal with insufficient number of GPUs.
        if n_requested_gpus > len(self._gpus):
            return None

        gpu_ids = self._gpus[:n_requested_gpus]
        self._gpus = self._gpus[n_requested_gpus:]

        return {"gpu_ids": gpu_ids}

    def release(self, acquired_resources):
        self._gpus += acquired_resources["gpu_ids"]
        acquired_resources.pop("gpu_ids", None)
