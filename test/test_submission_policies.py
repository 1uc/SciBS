import scibs
import contextlib
import io

import pytest


def test_subprocess_policy():
    policy = scibs.SubprocessSubmissionPolicy()
    policy(".", ["pwd"])


def test_stdout_policy():
    policy = scibs.StdOutSubmissionPolicy()

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        policy(".", ["pwd"])

    # strip trailing newline.
    text = stdout.getvalue()[:-1]
    expected = "cd . && pwd && cd -"

    assert text == expected


def test_stdout_nocwd():
    policy = scibs.StdOutSubmissionPolicy()

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        policy(None, ["pwd"])

    # strip trailing newline.
    text = stdout.getvalue()[:-1]
    expected = "pwd"

    assert text == expected


class IncompleteSubmissionPolicy(scibs.SubmissionPolicy):
    pass


def test_policy_interface():
    policy = IncompleteSubmissionPolicy()

    with pytest.raises(NotImplementedError):
        policy(".", ["pwd"])


def test_multi_policy():
    dbg_policy = scibs.DebugSubmissionPolicy()
    stdout_policy = scibs.StdOutSubmissionPolicy()

    policy = scibs.MultiSubmissionPolicy([stdout_policy, dbg_policy])

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        policy(".", ["pwd"])

    # strip trailing newline.
    text = stdout.getvalue()[:-1]
    expected = "cd . && pwd && cd -"

    assert text == expected

    assert dbg_policy.cwd == "."
    assert dbg_policy.cmd == ["pwd"]
