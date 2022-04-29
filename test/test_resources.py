# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import datetime
import scibs

import pytest


def test_mpi_resource():
    n_tasks = 10
    mem_per_task = 50 * 10**6
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
    total_memory = 50 * 10**6
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
    mem_per_cu = 50 * 10**6
    cu = scibs.CU(n_omp_threads=n_threads, n_mpi_tasks=n_tasks)

    r = scibs.CUResource(cu, n_cus, mem_per_cu=mem_per_cu)

    assert r.n_cores == n_threads * n_cus
    assert r.memory_per_core == mem_per_cu / n_threads
    assert r.needs_mpi
    assert r.needs_omp


class IncompleteResource(scibs.Resource):
    pass


def test_resource_interface():
    incomplete_resource = IncompleteResource()

    with pytest.raises(NotImplementedError):
        incomplete_resource.n_cores

    with pytest.raises(NotImplementedError):
        incomplete_resource.memory_per_core


def test_cu_omp():
    cu = scibs.CU(n_omp_threads=2)
    assert cu.n_cores_per_cu == 2

    cu = scibs.CU(n_mpi_tasks=3)
    assert cu.n_cores_per_cu == 3


def test_just_cores_resource():
    just_cores = scibs.JustCoresResource()
    assert just_cores.n_cores == 1

    just_cores = scibs.JustCoresResource()
    assert just_cores.memory_per_core is None

    just_cores = scibs.JustCoresResource(n_cores=2, total_memory=4)
    assert just_cores.memory_per_core == 2.0


def test_just_gpus_resources():
    r = scibs.JustGPUsResource(3)
    assert r.n_cores == 1
    assert r.n_gpus_per_process == 3
    assert r.memory_per_core is None

    r = scibs.JustGPUsResource(n_gpus=4, total_memory=4)
    assert r.n_cores == 1
    assert r.n_gpus_per_process == 4
    assert r.memory_per_core == 4.0
