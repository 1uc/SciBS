import os

import scibs


def test_job():
    cmd = ["foo", "--bar"]
    wd = "${HOME}"

    resources = None
    name = "foo_bar"

    job = scibs.Job(cmd, resources, name=name, cwd=wd)

    assert job.cmd == cmd
    assert job.cwd == os.path.expandvars(wd)
    assert job.resources == resources
    assert job.name == name
