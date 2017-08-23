from context import cl


def test_query_nodes():
    assert cl.query_nodes() == 0


def test_query_jobs():
    assert cl.query_jobs(minutes=5) == 0
