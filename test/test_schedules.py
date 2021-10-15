import scibs
import datetime

import pytest


def test_local_gpu_resource():
    available_gpus = [1, 2, 3, 5]
    available_gpus_str = ",".join(map(str,available_gpus))

    lr = scibs.LocalGPUResources(available_gpus)
    assert lr._gpus == available_gpus

    lr = scibs.LocalGPUResources(available_gpus_str)
    assert lr._gpus == available_gpus

    job_1gpu = scibs.JustGPUsResource(n_gpus=1)
    job_2gpu = scibs.JustGPUsResource(n_gpus=2)
    job_5gpu = scibs.JustGPUsResource(n_gpus=5)

    assert lr.acquire(job_5gpu) is None

    r1 = lr.acquire(job_1gpu)
    r2 = lr.acquire(job_1gpu)
    r3 = lr.acquire(job_2gpu)
    r4 = lr.acquire(job_1gpu)

    assert r1 is not None
    assert r2 is not None
    assert r3 is not None
    assert r4 is None

    lr.release(r3)
    r5 = lr.acquire(job_1gpu)
    r6 = lr.acquire(job_1gpu)
    r7 = lr.acquire(job_1gpu)

    assert r5 is not None
    assert r6 is not None
    assert r7 is None

    lr.release(r1)
    lr.release(r2)
    # lr.release(r3)
    # lr.release(r4)
    lr.release(r5)
    lr.release(r6)
    # lr.release(r7)

    assert sorted(lr._gpus) == sorted(available_gpus)
