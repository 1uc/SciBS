# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import subprocess
import re


class SubmissionPolicy:
    def __call__(self, cmd, cwd, env):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `__call__`."
        )


class SubprocessSubmissionPolicy(SubmissionPolicy):
    def __init__(self, subprocess_kwargs=None):
        if subprocess_kwargs is None:
            subprocess_kwargs = {"check": True}

        self._kwargs = subprocess_kwargs

    def __call__(self, cmd, cwd, env):
        subprocess.run(cmd, **self._kwargs, cwd=cwd, env=env)


class StdOutSubmissionPolicy(SubmissionPolicy):
    def __call__(self, cmd, cwd, env):
        if cwd is None:
            print(" ".join(cmd))
        else:
            print(f"cd {cwd} && " + " ".join(cmd) + " && cd -")


class DebugSubmissionPolicy(SubmissionPolicy):
    def __call__(self, cmd, cwd, env):
        self.cmd = cmd
        self.cwd = cwd
        self.env = env


class MultiSubmissionPolicy(SubmissionPolicy):
    def __init__(self, policies):
        self._policies = policies

    def __call__(self, cmd, cwd, env):
        for policy in self._policies:
            policy(cmd, cwd, env)


class SLURMSubmissionPolicy(SubmissionPolicy):
    def __init__(self, subprocess_kwargs=None):
        if subprocess_kwargs is None:
            subprocess_kwargs = dict()

        self._kwargs = subprocess_kwargs
        self._kwargs["check"] = True
        self._kwargs["capture_output"] = True
        self._kwargs["encoding"] = "utf-8"

        self._job_ids = []

    def __call__(self, cmd, cwd, env):
        cp = subprocess.run(cmd, **self._kwargs, cwd=cwd, env=env)
        self._job_ids.append(self.parse_stdout(cp.stdout))

    def parse_stdout(self, str):
        for line in str.split("\n"):
            print(f"'{line}'")
            m = re.match("^Submitted batch job ([0-9]*)$", line)
            if m:
                return int(m.group(1))

        raise RuntimeError("Could not determine the job ID.")

    def previous_job_id(self):
        return self._job_ids[-1] if self._job_ids else None
