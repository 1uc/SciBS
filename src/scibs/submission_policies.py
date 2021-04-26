import subprocess


class SubmissionPolicy:
    def __call__(self, cwd, cmd):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `__call__`."
        )


class SubprocessSubmissionPolicy(SubmissionPolicy):
    def __init__(self, subprocess_kwargs=None):
        if subprocess_kwargs is None:
            subprocess_kwargs = {"check": True}

        self._kwargs = subprocess_kwargs

    def __call__(self, cwd, cmd):
        subprocess.run(cmd, **self._kwargs)


class StdOutSubmissionPolicy(SubmissionPolicy):
    def __call__(self, cwd, cmd):
        print(f"cd {cwd} && " + " ".join(cmd) + " && cd -")


class DebugSubmissionPolicy(SubmissionPolicy):
    def __call__(self, cwd, cmd):
        self.cmd = cmd
        self.cwd = cwd
