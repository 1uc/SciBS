import scibs


class Dependency:
    pass


class Singleton(Dependency):
    pass


class DependencyWithJobID(Dependency):
    def __init__(self, job_id):
        self._job_id = job_id

    @property
    def job_id(self):
        return self._job_id


class AfterOK(DependencyWithJobID):
    def __init__(self, job_id):
        super().__init__(job_id=job_id)


class AfterAny(DependencyWithJobID):
    def __init__(self, job_id):
        super().__init__(job_id=job_id)
