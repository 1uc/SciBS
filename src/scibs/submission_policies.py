# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import subprocess


class SubmissionPolicy:
    def __call__(self, cmd, cwd):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `__call__`."
        )


class SubprocessSubmissionPolicy(SubmissionPolicy):
    def __init__(self, subprocess_kwargs=None):
        if subprocess_kwargs is None:
            subprocess_kwargs = {"check": True}

        self._kwargs = subprocess_kwargs

    def __call__(self, cmd, cwd):
        subprocess.run(cmd, **self._kwargs, cwd=cwd)


class StdOutSubmissionPolicy(SubmissionPolicy):
    def __call__(self, cmd, cwd):
        if cwd is None:
            print(" ".join(cmd))
        else:
            print(f"cd {cwd} && " + " ".join(cmd) + " && cd -")


class DebugSubmissionPolicy(SubmissionPolicy):
    def __call__(self, cmd, cwd):
        self.cmd = cmd
        self.cwd = cwd


class MultiSubmissionPolicy(SubmissionPolicy):
    def __init__(self, policies):
        self._policies = policies

    def __call__(self, cmd, cwd):
        for policy in self._policies:
            policy(cmd, cwd)
