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


def test_lsf_mpi(mpi_resources):
    job = scibs.Job(["foo", "--bar"], mpi_resources, name="foo_bar")

    # fmt: off
    expected = [
        "bsub",
        "-J", "foo_bar",
        "-W", "03:00",
        "-R", "rusage[mem=50]",
        "-n", "10",
        "mpirun -np 10 foo --bar",
    ]
    # fmt: on

    # No real need to use contex management. LSF is just sends the instructions
    # to the actual batch system.
    lsf = scibs.LSF()
    assert lsf.cmdline(job) == expected


def test_local_mpi(mpi_resources):
    job = scibs.Job(["foo", "--bar"], mpi_resources, name="foo_bar")

    expected = "mpirun -np 10 foo --bar"

    # To be safe let's use the context manager.
    with scibs.LocalBS() as local:
        assert local.cmdline(job) == expected


@pytest.fixture
def mpi_omp_resources():
    n_threads = 4
    n_tasks = 1
    n_cus = 3
    mem_per_cu = 48 * 10 ** 6
    cu = scibs.CU(n_omp_threads=n_threads, n_mpi_tasks=n_tasks)

    return scibs.CUResource(cu, n_cus, mem_per_cu=mem_per_cu)


def test_eulerlsf_mpi_omp(mpi_omp_resources):
    job = scibs.Job(["foo", "--bar"], mpi_omp_resources, name="foo_bar")

    lsf = scibs.EulerLSF()

    # fmt: off
    expected = [
        "bsub",
        "-J", "foo_bar",
        "-R", "rusage[mem=12]",
        "-n", "12",
        "-R", "span[ptile=12]",
        "export OMP_NUM_THREADS=4; unset LSF_AFFINITY_HOSTFILE; mpirun -np 3 --map-by node:PE=4 foo --bar"
    ]
    # fmt: on

    assert lsf.cmdline(job) == expected


@pytest.fixture
def omp_resources():
    n_threads = 6
    mem = 120 * 10 ** 6
    return scibs.OMPResource(n_omp_threads=n_threads, total_memory=mem)


def test_eulerlsf_omp(omp_resources):
    wd = "wd"
    job = scibs.Job(["foo", "--bar"], omp_resources, name="foo_bar", cwd=wd)

    dbg_policy = scibs.DebugSubmissionPolicy()
    lsf = scibs.EulerLSF(submission_policy=dbg_policy)

    # fmt: off
    expected = [
        "bsub",
        "-J", "foo_bar",
        "-R", "rusage[mem=20]",
        "-n", "6",
        "-R", "span[ptile=6]",
        "export OMP_NUM_THREADS=6; foo --bar"
    ]
    # fmt: on

    lsf.submit(job)
    assert dbg_policy.cmd == expected
    assert dbg_policy.cwd == wd


def test_local_omp(omp_resources):
    wd = "wd"
    job = scibs.Job(["foo", "--bar"], omp_resources, name="foo_bar", cwd=wd)

    dbg_policy = scibs.DebugSubmissionPolicy()
    local = scibs.LocalBS(submission_policy=dbg_policy)

    expected = "export OMP_NUM_THREADS=6; foo --bar"

    local.submit(job)
    assert dbg_policy.cmd == expected
    assert dbg_policy.cwd == wd


class IncompleteSciBS(scibs.SciBS):
    pass


def test_scibs_interface(omp_resources):
    job = scibs.Job(["foo", "--bar"], omp_resources, name="foo_bar")

    bs = IncompleteSciBS()
    with pytest.raises(NotImplementedError):
        bs.submit(job)
