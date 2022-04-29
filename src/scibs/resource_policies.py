# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import os

class ResourcePolicy:
    """Handle resources specifically allocated to one job.

    While each job asks for the resources it needs, there are cases where not
    just the raw amount but the actual resource matters too. Typical example are
    GPU, while a job may ask for 3 out of 8 GPU on a node, it is important to
    assign 3 specific GPUs to that job.

    `ResourcePolicy` are policies make sure that the job runs on the specific
    resources that have been allocated to it.
    """
    def __call__(self, job, acquired_resources):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `__call__`."
        )

class DefaultResourcePolicy(ResourcePolicy):
    def __call__(self, job, acquired_resources):
        pass

class GPUResourcePolicy(ResourcePolicy):
    def __call__(self, job, acquired_resources):
        gpu_ids = acquired_resources["gpu_ids"]
        gpu_ids = ",".join(map(str, gpu_ids))

        if job.env is None:
            job.env = dict(os.environ)

        job.env["CUDA_VISIBLE_DEVICES"] = gpu_ids

