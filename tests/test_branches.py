from lakefs_client.models import (
    BranchCreation,
    CherryPickCreation,
    ResetCreation,
    RevertCreation,
)

from prefect_lakefs.branches import (
    cherry_pick,
    create_branch,
    delete_branch,
    diff_branch,
    get_branch,
    list_branches,
    reset_branch,
    revert_branch,
)


async def test_list_branches(lakefs_credentials, _mock_branches_client):
    await list_branches.fn(repository="example", lakefs_credentials=lakefs_credentials)
    # TODO: remove this comment.
    #       Aslong as the call args match we don't need to explicitly mock the resp.
    #       relying on openapi client generator.
    assert _mock_branches_client.list_branches.call_args[1]["repository"] == "example"


async def test_create_branch(lakefs_credentials, _mock_branches_client):
    await create_branch.fn(
        repository="example",
        name="feature",
        source="main",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.create_branch.call_args[1]["repository"] == "example"
    assert _mock_branches_client.create_branch.call_args[1][
        "branch_creation"
    ] == BranchCreation(name="feature", source="main")


async def test_cherry_pick(lakefs_credentials, _mock_branches_client):
    await cherry_pick.fn(
        repository="example",
        branch="main",
        cherry_pick_ref="exp",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.cherry_pick.call_args[1]["repository"] == "example"
    assert _mock_branches_client.cherry_pick.call_args[1]["branch"] == "main"
    assert _mock_branches_client.cherry_pick.call_args[1][
        "cherry_pick_creation"
    ] == CherryPickCreation(ref="exp")


async def test_delete_branch(lakefs_credentials, _mock_branches_client):
    await delete_branch.fn(
        repository="example",
        branch="feature",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.delete_branch.call_args[1]["repository"] == "example"
    assert _mock_branches_client.delete_branch.call_args[1]["branch"] == "feature"


async def test_diff_branch(lakefs_credentials, _mock_branches_client):
    await diff_branch.fn(
        repository="example",
        branch="feature",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.diff_branch.call_args[1]["repository"] == "example"
    assert _mock_branches_client.diff_branch.call_args[1]["branch"] == "feature"


async def test_get_branch(lakefs_credentials, _mock_branches_client):
    await get_branch.fn(
        repository="example",
        branch="feature",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.get_branch.call_args[1]["repository"] == "example"
    assert _mock_branches_client.get_branch.call_args[1]["branch"] == "feature"


async def test_reset_branch(lakefs_credentials, _mock_branches_client):
    await reset_branch.fn(
        repository="example",
        branch="feature",
        reset_type="reset",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.reset_branch.call_args[1]["repository"] == "example"
    assert _mock_branches_client.reset_branch.call_args[1]["branch"] == "feature"
    assert _mock_branches_client.reset_branch.call_args[1][
        "reset_creation"
    ] == ResetCreation(type="reset")


async def test_revert_branch(lakefs_credentials, _mock_branches_client):
    await revert_branch.fn(
        repository="example",
        branch="feature",
        revert_ref="exp_sha",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_branches_client.revert_branch.call_args[1]["repository"] == "example"
    assert _mock_branches_client.revert_branch.call_args[1]["branch"] == "feature"
    assert _mock_branches_client.revert_branch.call_args[1][
        "revert_creation"
    ] == RevertCreation(ref="exp_sha", parent_number=1)
