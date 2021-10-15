import scibs
import pytest


def test_local_bs_with_gpus():
    r = scibs.JustGPUsResource(n_gpus=1)
    j = scibs.Job(
        cmd=["echo ${CUDA_VISIBLE_DEVICES} >> __dbg_cuda_devices.txt"], resources=r
    )

    local_bs_kwargs = {
        "resource_policy": scibs.GPUResourcePolicy(),
        "local_resources": scibs.LocalGPUResources("1,2,3,4,5,6"),
    }

    with scibs.LocalBS(**local_bs_kwargs) as queue:
        for k in range(10):
            queue.submit(j)
