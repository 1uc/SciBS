# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import scibs
import contextlib
import io

import pytest


def test_subprocess_policy():
    policy = scibs.SubprocessSubmissionPolicy()
    policy(["pwd"], cwd=".", env=None)


def test_stdout_policy():
    policy = scibs.StdOutSubmissionPolicy()

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        policy(["pwd"], cwd=".", env=None)

    # strip trailing newline.
    text = stdout.getvalue()[:-1]
    expected = "cd . && pwd && cd -"

    assert text == expected


def test_stdout_nocwd():
    policy = scibs.StdOutSubmissionPolicy()

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        policy(["pwd"], None, env=None)

    # strip trailing newline.
    text = stdout.getvalue()[:-1]
    expected = "pwd"

    assert text == expected


class IncompleteSubmissionPolicy(scibs.SubmissionPolicy):
    pass


def test_policy_interface():
    policy = IncompleteSubmissionPolicy()

    with pytest.raises(NotImplementedError):
        policy(["pwd"], cwd=".", env=None)


def test_multi_policy():
    dbg_policy = scibs.DebugSubmissionPolicy()
    stdout_policy = scibs.StdOutSubmissionPolicy()

    policy = scibs.MultiSubmissionPolicy([stdout_policy, dbg_policy])

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        policy(["pwd"], cwd=".", env=None)

    # strip trailing newline.
    text = stdout.getvalue()[:-1]
    expected = "cd . && pwd && cd -"

    assert text == expected

    assert dbg_policy.cwd == "."
    assert dbg_policy.cmd == ["pwd"]
    assert dbg_policy.env == None


def test_slurm_submission_policy_parse():
    output = """
sbatch: INFO: Activating auto partition selection plugin, please report errors to HPC/CS
sbatch: INFO: Job specifies cpu constraint, setting --constraint=[skl|clx]
Submitted batch job 408469
"""

    policy = scibs.SLURMSubmissionPolicy()
    job_id = policy.parse_stdout(output)
    assert job_id == 408469
