import scibs


def test_async_local_bs():
    jobs = [
        scibs.Job(["pwd"], scibs.MPIResource(n_mpi_tasks=n), name=f"PWD-{k}")
        for k, n in enumerate([1, 1, 1, 2, 4, 6, 2, 1, 1])
    ]

    with scibs.AsyncLocalBS() as bs:
        for job in jobs:
            bs.submit(job)
