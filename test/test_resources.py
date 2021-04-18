import datetime
import scibs


def test_mpi_resource():
    n_tasks = 10
    mem_per_task = 50 * 10 ** 6
    wall_clock = datetime.timedelta(hours=3)
    r = scibs.MPIResource(
        n_mpi_tasks=n_tasks, wall_clock=wall_clock, mem_per_task=mem_per_task
    )

    assert r.n_cores == n_tasks
    assert r.n_mpi_tasks == n_tasks
    assert r.wall_clock == wall_clock
    assert r.memory_per_core == mem_per_task
    assert r.needs_mpi
    assert not r.needs_omp

    assert not hasattr(r, "n_omp_threads")


def test_omp_resource():
    n_threads = 8
    total_memory = 50 * 10 ** 6
    wall_clock = datetime.timedelta(hours=3)
    r = scibs.OMPResource(
        n_omp_threads=n_threads, wall_clock=wall_clock, total_memory=total_memory
    )

    assert r.n_cores == n_threads
    assert r.n_omp_threads == n_threads
    assert r.wall_clock == wall_clock
    assert r.memory_per_core == total_memory / n_threads
    assert not r.needs_mpi
    assert r.needs_omp

    assert not hasattr(r, "n_mpi_tasks")


def test_mpi_omp_resource():
    n_threads = 4
    n_tasks = 1
    n_cus = 3
    mem_per_cu = 50 * 10 ** 6
    cu = scibs.CU(n_omp_threads=n_threads, n_mpi_tasks=n_tasks)

    r = scibs.CUResource(cu, n_cus, mem_per_cu=mem_per_cu)

    assert r.n_cores == n_threads * n_cus
    assert r.memory_per_core == mem_per_cu / n_threads
    assert r.needs_mpi
    assert r.needs_omp
