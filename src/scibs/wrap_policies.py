class WrapPolicy:
    def __call__(self, job):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `__call__`."
        )


class DefaultWrapPolicy(WrapPolicy):
    """Linux friendly defaults for running simple jobs."""

    def __call__(self, job):
        r = job.resources

        if r.needs_mpi and not r.needs_omp:
            return self.wrap_cmd_mpi(job)

        elif r.needs_mpi and r.needs_omp:
            return self.wrap_cmd_mpi_omp(job)

        elif not r.needs_mpi and r.needs_omp:
            return self.wrap_cmd_omp(job)

        else:
            return self.wrap_cmd_default(job.cmd)

    def wrap_cmd_mpi(self, job):
        r = job.resources
        return f"mpirun -np {r.n_mpi_tasks} " + " ".join(job.cmd)

    def wrap_cmd_mpi_omp(self, job):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `wrap_cmd_mpi_omp`."
        )

    def wrap_cmd_omp(self, job):
        r = job.resources
        return " ".join([f"export OMP_NUM_THREADS={r.n_omp_threads};"] + job.cmd)

    def wrap_cmd_default(self, job):
        return " ".join(job.cmd)


class EulerWrapPolicy(DefaultWrapPolicy):
    def wrap_cmd_mpi_omp(self, job):
        cmd = job.cmd
        r = job.resources

        return " ".join(
            [
                f"export OMP_NUM_THREADS={r.n_omp_threads};",
                "unset LSF_AFFINITY_HOSTFILE;",
                f"mpirun -np {r.n_mpi_tasks} --map-by node:PE={r.n_omp_threads}",
            ]
            + cmd
        )
