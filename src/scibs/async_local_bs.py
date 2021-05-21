import asyncio
import psutil

import scibs


class AsyncIOSubmissionPolicy(scibs.SubmissionPolicy):
    def __init__(self, subprocess_kwargs=None):
        if subprocess_kwargs is None:
            subprocess_kwargs = {"check": False}

        self._kwargs = subprocess_kwargs

    def __call__(self, cwd, cmd):
        return asyncio.create_subprocess_shell(cmd, cwd=cwd, **self._kwargs)


class AsyncLocalBS(scibs.SciBS):
    """An ad hoc local batch system for when no batch system is present.

    Instead of submitting jobs to a batch system, those jobs will simply
    be run on the same computer.

    NOTE: By default, this will, in the simplest case, run
             subprocess.run(" ".join(job.cmd), shell=True, check=False)

          The important thing to observe is the `shell=True` part.
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
            #    with AsyncLocalBS() as local_bs:
            #        local_bs.submit(...)
            #        ...
            #        local_bs.submit(...)
            raise RuntimeError(f"{__class__} is missing context.")

    def _run_all(self):
        self._ensure_with_context()

        local_resources = LocalResources()
        job_schedule = GreedySchedule(local_resources, self._context["jobs"])
        asyncio.run(schedule_jobs(self._wrap_policy, job_schedule))


class LocalResources:
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
        self._cores += acquired_resources["cores"]


class Schedule:
    def empty(self):
        """Has everything been scheduled?"""
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `empty`."
        )

    def next_job(self):
        """Returns the tuple `(job_id, job)` for the next job to be run.

        It is possible that there are not enough resources to run a further
        job, in this case None is returned instead.
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
    def __init__(self, local_resources, jobs):
        self._local_resources = local_resources
        self._jobs = jobs
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
            return next(
                iter(
                    (job_id, self._jobs[job_id])
                    for job_id in self._unscheduled_jobs
                    if acquire(job_id) is not None
                )
            )

        except StopIteration:
            return None

    def complete(self, job_id):
        self._job_info[job_id]["complete"] = True
        self._local_resources.release(self._job_info[job_id]["resources"])


async def _launch_job(wrap_policy, job_id, job):
    cmd = wrap_policy(job)
    proc = await asyncio.create_subprocess_shell(cmd, cwd=job.cwd)
    await proc.wait()
    return {"job_id": job_id}


async def schedule_jobs(wrap_policy, job_schedule):
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
