from prefect_lakefs.refs import (
    diff_refs,
    find_merge_base,
    log_commits,
    merge_into_branch,
)


async def test_diff_refs(lakefs_credentials, _mock_refs_client):
    await diff_refs.fn(
        repository="example",
        left_ref="left",
        right_ref="right",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_refs_client.diff_refs.call_args[1]["repository"] == "example"
    assert _mock_refs_client.diff_refs.call_args[1]["left_ref"] == "left"
    assert _mock_refs_client.diff_refs.call_args[1]["right_ref"] == "right"


async def test_find_merge_base(lakefs_credentials, _mock_refs_client):
    await find_merge_base.fn(
        repository="example",
        source_ref="source",
        destination_branch="dest",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_refs_client.find_merge_base.call_args[1]["repository"] == "example"
    assert _mock_refs_client.find_merge_base.call_args[1]["source_ref"] == "source"
    assert (
        _mock_refs_client.find_merge_base.call_args[1]["destination_branch"] == "dest"
    )


async def test_log_commits(lakefs_credentials, _mock_refs_client):
    await log_commits.fn(
        repository="example",
        ref="ref",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_refs_client.log_commits.call_args[1]["repository"] == "example"
    assert _mock_refs_client.log_commits.call_args[1]["ref"] == "ref"


async def test_merge_into_branch(lakefs_credentials, _mock_refs_client):
    await merge_into_branch.fn(
        repository="example",
        source_ref="source",
        destination_branch="dest",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_refs_client.merge_into_branch.call_args[1]["repository"] == "example"
    assert _mock_refs_client.merge_into_branch.call_args[1]["source_ref"] == "source"
    assert (
        _mock_refs_client.merge_into_branch.call_args[1]["destination_branch"] == "dest"
    )
