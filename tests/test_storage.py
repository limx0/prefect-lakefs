from io import BytesIO
from unittest.mock import MagicMock, patch

from prefect import flow, task

from prefect_lakefs.storage import LakeFS, LakeFSCredentials


def test_storage_init():
    # Arrange
    storage = LakeFS(
        bucket_name="",
        credentials=LakeFSCredentials(
            repository="data", branch="dev", commit_on_task=True
        ),
    )

    @task()
    def my_task():
        return 1

    @flow(result_storage=storage, persist_result=True)
    def my_flow():
        my_task()

    mock_client = MagicMock()

    # Act
    with patch("prefect_lakefs.credentials.LakeFSCredentials.get_client") as mock_creds:
        mock_creds.return_value.__enter__.return_value = mock_client
        my_flow()

    result = mock_client.method_calls[1].kwargs
    assert result["repository"] == "data"
    assert result["branch"] == "dev"
    assert isinstance(result["path"], str)
    assert isinstance(result["content"], BytesIO)
