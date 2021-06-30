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
    with scibs.SequentialLocalBS() as local:
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


@pytest.fixture
def just_cores_resource():
    n_cores = 16
    mem = 16 * 10 ** 6

    return scibs.JustCoresResource(n_cores=n_cores, total_memory=mem)


def test_just_cores_resource(just_cores_resource):
    wd = "wd"
    job = scibs.Job(["foo", "--bar"], just_cores_resource, name="foo_bar", cwd=wd)

    dbg_policy = scibs.DebugSubmissionPolicy()
    lsf = scibs.EulerLSF(submission_policy=dbg_policy)

    # fmt: off
    expected = [
        "bsub",
        "-J", "foo_bar",
        "-R", "rusage[mem=1]",
        "-n", "16",
        "-R", "span[ptile=16]",
        "foo --bar"
    ]
    # fmt: on

    lsf.submit(job)
    assert dbg_policy.cmd == expected
    assert dbg_policy.cwd == wd


class IncompleteSciBS(scibs.SciBS):
    pass


def test_scibs_interface(omp_resources):
    job = scibs.Job(["foo", "--bar"], omp_resources, name="foo_bar")

    bs = IncompleteSciBS()
    with pytest.raises(NotImplementedError):
        bs.submit(job)


def test_sequential_omp(omp_resources):
    wd = "wd"
    job = scibs.Job(["foo", "--bar"], omp_resources, name="foo_bar", cwd=wd)

    dbg_policy = scibs.DebugSubmissionPolicy()
    local = scibs.SequentialLocalBS(submission_policy=dbg_policy)

    expected = "export OMP_NUM_THREADS=6; foo --bar"

    local.submit(job)
    assert dbg_policy.cmd == expected
    assert dbg_policy.cwd == wd


def test_local_bs():
    # fmt: off
    jobs = [
        scibs.Job(
            ["pwd"],
            scibs.MPIResource(n_mpi_tasks=n, wall_clock=datetime.timedelta(hours=h)),
            name=f"PWD-{k}",
        )
        for k, (n, h) in enumerate(
            zip([1, 1, 1, 2, 4, 6, 2, 1, 1],
                [2, 1, 3, 4, 23, 3, 2, 5, 3])
        )
    ]
    # fmt: on

    with scibs.LocalBS() as bs:
        for job in jobs:
            bs.submit(job)
