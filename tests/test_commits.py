from datetime import datetime

from lakefs_client.models import CommitCreation

from prefect_lakefs.commits import commit, get_commit


async def test_commit(lakefs_credentials, _mock_commits_client):
    await commit.fn(
        repository="example",
        branch="main",
        message="test_commit",
        metadata=dict(),
        date=int(datetime(2023, 5, 13, 0, 0, 0).timestamp()),
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_commits_client.commit.call_args[1]["repository"] == "example"
    assert _mock_commits_client.commit.call_args[1]["branch"] == "main"
    assert _mock_commits_client.commit.call_args[1][
        "commit_creation"
    ] == CommitCreation(
        message="test_commit",
        metadata=dict(),
        date=int(datetime(2023, 5, 13, 0, 0, 0).timestamp()),
    )


async def test_get_commit(lakefs_credentials, _mock_commits_client):
    await get_commit.fn(
        repository="example",
        commit_id="commit_sha",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_commits_client.get_commit.call_args[1]["repository"] == "example"
    assert _mock_commits_client.get_commit.call_args[1]["commit_id"] == "commit_sha"
