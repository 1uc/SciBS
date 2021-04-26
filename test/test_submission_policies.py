import scibs

import pytest


def test_subprocess_policy():
    policy = scibs.SubprocessSubmissionPolicy()
    policy(".", "pwd")


def test_stdout_policy():
    policy = scibs.StdOutSubmissionPolicy()
    policy(".", "pwd")


class IncompleteSubmissionPolicy(scibs.SubmissionPolicy):
    pass


def test_policy_interface():
    policy = IncompleteSubmissionPolicy()

    with pytest.raises(NotImplementedError):
        policy(".", "pwd")
