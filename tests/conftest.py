from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from lakefs_client.apis import BranchesApi
from prefect.testing.utilities import prefect_test_harness

from prefect_lakefs import LakeFSCredentials


@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    """
    Sets up test harness for temporary DB during test runs.
    """
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def reset_object_registry():
    """
    Ensures each test has a clean object registry.
    """
    from prefect.context import PrefectObjectRegistry

    with PrefectObjectRegistry():
        yield


@pytest.fixture
def lakefs_credentials():
    return LakeFSCredentials(
        endpoint_url="http://localhost:8000/api/v1",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )


@pytest.fixture
def _mock_branches_client(monkeypatch):
    branches_client = MagicMock(spec=BranchesApi)

    @contextmanager
    def get_client(self, _):
        yield branches_client

    monkeypatch.setattr(
        "prefect_lakefs.credentials.LakeFSCredentials.get_client",
        get_client,
    )

    return branches_client
