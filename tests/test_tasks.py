from prefect import flow

from prefect_lakefs.tasks import (
    goodbye_prefect_lakefs,
    hello_prefect_lakefs,
)


def test_hello_prefect_lakefs():
    @flow
    def test_flow():
        return hello_prefect_lakefs()

    result = test_flow()
    assert result == "Hello, prefect-lakefs!"


def goodbye_hello_prefect_lakefs():
    @flow
    def test_flow():
        return goodbye_prefect_lakefs()

    result = test_flow()
    assert result == "Goodbye, prefect-lakefs!"
