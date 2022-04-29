import scibs


def test_singleton():
    policy = scibs.SLURMDependencyPolicy()

    assert policy(scibs.Singleton()) == "--dependency=singleton"
    assert policy(scibs.AfterAny(123)) == "--dependency=afterany:123"
    assert policy(scibs.AfterOK(321)) == "--dependency=afterok:321"
