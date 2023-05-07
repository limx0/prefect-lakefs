import pytest
from lakefs_client.apis import BranchesApi, CommitsApi, ObjectsApi


@pytest.mark.parametrize(
    "resource_type,client_type",
    [
        ("branches", BranchesApi),
        ("commits", CommitsApi),
        ("objects", ObjectsApi),
    ],
)
def test_client_return_type(lakefs_credentials, resource_type, client_type):
    with lakefs_credentials.get_client(resource_type) as client:
        assert isinstance(client, client_type)


def test_client_bad_resource_type(lakefs_credentials):
    with pytest.raises(ValueError, match="Invalid client type provided 'foo-bar'"):
        with lakefs_credentials.get_client("foo-bar"):
            pass
