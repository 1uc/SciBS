# SPDX-License-Identifier: MIT
# Copyright (c) 2021 ETH Zurich, Luc Grosheintz-Laval

import os


class Job:
    """A job without the decorations required by the BS."""

    def __init__(self, cmd, resources, cwd=None, env=None, name=None):
        self._cwd = os.path.expandvars(cwd) if cwd is not None else None
        self._cmd = cmd
        self._env = env
        self._resources = resources
        self._name = name

    @property
    def cwd(self):
        """When the job runs, this should be its `${PWD}`."""
        return self._cwd

    @property
    def cmd(self):
        """The `subprocess`-style (list of strings) command to be launched.

        If there where no batch system, you'd do something akin to

            subprocess.run(cmd)
        """

        return self._cmd

    @property
    def env(self):
        """The shell environment that should be used."""

        return self._env

    @env.setter
    def env(self, env):
        """The shell environment that should be used."""

        self._env = env

    @property
    def resources(self):
        """These are the resources required to run this job.

        See `scibs.Resources` for various options.
        """
        return self._resources

    @property
    def name(self):
        """Human-friendly name of the job."""
        return self._name
