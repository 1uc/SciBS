class SciBS:
    """A Scientific Batch System."""

    def submit(self, job):
        raise NotImplementedError(
            f"{self.__class__.__name__} hasn't implemented `submit`."
        )
