import scibs
import datetime

import pytest


@pytest.fixture
def three_hours():
    return datetime.timedelta(hours=3)


@pytest.fixture
def mpi_resources(three_hours):
    n_tasks = 10
    mem_per_task = 50 * 10 ** 6
    return scibs.MPIResource(
        n_mpi_tasks=n_tasks, wall_clock=three_hours, mem_per_task=mem_per_task
    )


@pytest.fixture
def omp_resources():
    n_threads = 6
    mem = 120 * 10 ** 6
    return scibs.OMPResource(n_omp_threads=n_threads, total_memory=mem)


@pytest.fixture
def mpi_omp_resources():
    n_threads = 4
    n_tasks = 1
    n_cus = 3
    mem_per_cu = 48 * 10 ** 6
    cu = scibs.CU(n_omp_threads=n_threads, n_mpi_tasks=n_tasks)

    return scibs.CUResource(cu, n_cus, mem_per_cu=mem_per_cu)


@pytest.fixture
def default_wrap_policy():
    return scibs.DefaultWrapPolicy()


def test_default_wrap_policy_mpi(mpi_resources, default_wrap_policy):
    job = scibs.Job(["foo", "--bar"], mpi_resources, name="foo_bar")

    assert default_wrap_policy(job) == "mpirun -np 10 foo --bar"


def test_default_wrap_policy_omp(omp_resources, default_wrap_policy):
    job = scibs.Job(["foo", "--bar"], omp_resources, name="foo_bar")

    expected = "export OMP_NUM_THREADS=6; foo --bar"
    assert default_wrap_policy(job) == expected


def test_default_wrap_policy_mpi_omp(mpi_omp_resources, default_wrap_policy):
    job = scibs.Job(["foo", "--bar"], mpi_omp_resources, name="foo_bar")

    with pytest.raises(NotImplementedError):
        default_wrap_policy(job)


def test_default_wrap_policy_default(default_wrap_policy):
    job = scibs.Job(["foo", "--bar"], scibs.JustCoresResource(), name="foo_bar")
    assert default_wrap_policy(job) == "foo --bar"


class IncompleteWrapPolicy(scibs.WrapPolicy):
    pass


def test_incomplete_wrap_policy():
    job = scibs.Job(["foo", "--bar"], None, name="foo_bar")

    incomplete_wrap_policy = IncompleteWrapPolicy()
    with pytest.raises(NotImplementedError):
        incomplete_wrap_policy(job)
