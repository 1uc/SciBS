# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval


class SciBS:
    """A Scientific Batch System.

    Note: Batch systems should be used inside a `with` block to allow the batch
    system to acquire and release resources properly.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def submit(self, job):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `submit`."
        )
