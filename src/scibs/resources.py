class Resource:
    """Abstract base for resources.

    `Resource`s should follow the following conventions:
        - wall_clock is always a `datetime.timedelta` object representing the
          amount wall-clock time the job is allowed to run for.

        - Any memory is always given in bytes.

    """

    @property
    def n_cores(self):
        """Total number of CPU cores requested."""
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `n_total_cores`."
        )

    @property
    def n_gpus_per_process(self):
        """Total number of GPUs requested per process."""
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `n_gpus_per_process`."
        )

    @property
    def memory_per_core(self):
        """Memory requested per core."""
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `memory_per_core`."
        )

    @property
    def needs_mpi(self):
        return hasattr(self, "n_mpi_tasks") and self.n_mpi_tasks

    @property
    def needs_omp(self):
        return hasattr(self, "n_omp_threads") and self.n_omp_threads

    @property
    def needs_gpus(self):
        return hasattr(self, "n_gpus_per_process") and self.n_gpus_per_process


class MPIResource(Resource):
    """A simple MPI job."""

    def __init__(self, n_mpi_tasks, wall_clock=None, mem_per_task=None):
        self.n_mpi_tasks = n_mpi_tasks
        self.wall_clock = wall_clock
        self._mem_per_task = mem_per_task

    @property
    def n_cores(self):
        return self.n_mpi_tasks

    @property
    def memory_per_core(self):
        return self._mem_per_task


class OMPResource(Resource):
    """A simple OpenMP job."""

    def __init__(self, n_omp_threads, wall_clock=None, total_memory=None):
        """Create an OpenMP resource requirement without frills.

        Args:
            n_omp_threads: number of OpenMP threads.
            wall_clock: wall-clock runtime allowance.
            total_memory: total bytes of RAM required to run this job.
        """

        self.n_omp_threads = n_omp_threads
        self.wall_clock = wall_clock
        self._total_memory = total_memory

    @property
    def n_cores(self):
        return self.n_omp_threads

    @property
    def memory_per_core(self):
        return self._total_memory / self.n_omp_threads


class CU:
    """A 'Compute Unit' (CU) is conceptually part of a node.

    When specifying resources for hybrid jobs, it's useful to think in
    multiples of, for lack of a better word, 'Compute Units'. A CU should
    represent a fraction of the resources available on each node of the
    cluster.

    This CU knows about MPI tasks, OpenMP threads and GPUs.

    Examples:
        1. A hybrid MPI/OpenMP simulation which wants to use 4 OMP threads per
        MPI tasks could define a CU consisting of 4 OMP threads and 1 MPI task.

        2. A hybrid MPI/CUDA simulation might ask for 1 MPI task and 1 GPU.

    Note: One OpenMP threads is assumed to run on the core assigned to the MPI
          task.
    """

    def __init__(self, n_mpi_tasks=None, n_omp_threads=None, n_gpus=None):
        self.n_mpi_tasks = n_mpi_tasks
        self.n_omp_threads = n_omp_threads
        self.n_gpus = n_gpus

    @property
    def n_cores_per_cu(self):
        if self.n_mpi_tasks is None:
            return self.n_omp_threads
        elif self.n_omp_threads is None:
            return self.n_mpi_tasks
        else:
            return max(self.n_mpi_tasks, self.n_omp_threads)


class CUResource(Resource):
    """A resource defined via a 'Compute Unit' (CU).

    A CU is an idealized collection of resources found on one node of the
    cluster, see `scibs.CU`.
    """

    def __init__(self, cu, n_cus, wall_clock=None, mem_per_cu=None):
        """
        Args:
            cu: A `scibs.CU` compute unit.
            n_cus:       Number of CU requested.
            wall_clock:  Wall-clock runtime allowance.
            mem_per_cu:  memory per CU.
        """

        self.cu = cu
        self.n_cus = n_cus
        self.wall_clock = wall_clock
        self.mem_per_cu = mem_per_cu

    @property
    def n_mpi_tasks(self):
        return self.cu.n_mpi_tasks * self.n_cus

    @property
    def n_omp_threads(self):
        return self.cu.n_omp_threads

    @property
    def n_cores(self):
        return self.cu.n_cores_per_cu * self.n_cus

    @property
    def memory_per_core(self):
        total_memory = self.mem_per_cu * self.n_cus
        return total_memory / self.n_cores


class JustGPUsResource(Resource):
    """Just request `n` GPUs and a core, no MPI or OpenMP implied."""

    def __init__(self, n_gpus, total_memory=None, wall_clock=None):
        self.wall_clock = wall_clock

        self._n_cores = 1
        self._n_gpus = n_gpus
        self._total_memory = total_memory

    @property
    def n_cores(self):
        return self._n_cores

    @property
    def n_gpus_per_process(self):
        return self._n_gpus

    @property
    def memory_per_core(self):
        if self._total_memory is None:
            return None

        return self._total_memory // self._n_cores


class JustCoresResource(Resource):
    def __init__(self, n_cores=1, total_memory=None, wall_clock=None):
        self.wall_clock = wall_clock

        self._n_cores = n_cores
        self._total_memory = total_memory

    @property
    def n_cores(self):
        return self._n_cores

    @property
    def memory_per_core(self):
        if self._total_memory is None:
            return None

        return self._total_memory // self._n_cores
