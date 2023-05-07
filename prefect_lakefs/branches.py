"""Module for interacting with LakeFS branches from Prefect flows."""
from typing import Any, Dict

from lakefs_client.models import (
    BranchCreation,
    CherryPickCreation,
    Commit,
    DiffList,
    Ref,
    RefList,
    ResetCreation,
    RevertCreation,
)
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_lakefs.credentials import LakeFSCredentials


@task
async def list_branches(
    repository: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> RefList:
    """list branches for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `RefList` object.

    Example:
        List branches for repository named "example":
        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import list_branches

        @flow
        def list_branches_for_example_repo():
            branches = list_branches(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
            )
        ```
    """

    # TODO: check for async_req flag and propagate accordingly.
    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.list_branches,
            repository=repository,
            **lakefs_kwargs,
        )


@task
async def create_branch(
    repository: str,
    name: str,
    lakefs_credentials: LakeFSCredentials,
    source: str = "main",
    **lakefs_kwargs: Dict[str, Any],
) -> str:
    """create branch for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        name: name of the branch to be created.
        source: Source ref id for the branch to be created.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        string contaiing the branch id.

    Example:
        Create branch named `feature` for repository named `example`:
        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import create_branch

        @flow
        def create_branch_for_example_repo():
            branch = create_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                name="feature",
                source="main",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.create_branch,
            repository=repository,
            branch_creation=BranchCreation(name=name, source=source),
            **lakefs_kwargs,
        )


@task
async def cherry_pick(
    repository: str,
    branch: str,
    cherry_pick_ref: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> Commit:
    """cherry-pick a ref and apply to branch for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to apply the changes.
        cherry_pick_ref: ref id of the changes to apply.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `Commit` object.

    Example:
        cheery-pick ref `exp_commit_sha` on branch named `feature` for
        repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import cherry_pick

        @flow
        def cherry_pick_for_example_repo():
            applied = cherry_pick(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="feature",
                cherry_pick_ref="exp_commit_sha",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.cherry_pick,
            repository=repository,
            branch=branch,
            cherry_pick_creation=CherryPickCreation(ref=cherry_pick_ref),
            **lakefs_kwargs,
        )


@task
async def delete_branch(
    repository: str,
    branch: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> None:
    """delete branch for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to delete.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        None

    Example:
        delete branch named `feature` for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import delete_branch

        @flow
        def delete_branch_for_example_repo():
            delete_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="feature",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.delete_branch,
            repository=repository,
            branch=branch,
            **lakefs_kwargs,
        )


@task
async def diff_branch(
    repository: str,
    branch: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> DiffList:
    """diff branch for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to fetch the diff list.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `DiffList` object.

    Example:
        get diff for a branch named `feature` for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import diff_branch

        @flow
        def diff_branch_for_example_repo():
            diff_list = diff_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="feature",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.diff_branch,
            repository=repository,
            branch=branch,
            **lakefs_kwargs,
        )


@task
async def get_branch(
    repository: str,
    branch: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> Ref:
    """get branch details for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to fetch the details.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `Ref` object.

    Example:
        get details for a branch named `feature` for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import get_branch

        @flow
        def get_branch_for_example_repo():
            branch = get_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="feature",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.get_branch,
            repository=repository,
            branch=branch,
            **lakefs_kwargs,
        )


@task
async def reset_branch(
    repository: str,
    branch: str,
    lakefs_credentials: LakeFSCredentials,
    reset_type: str = "reset",
    **lakefs_kwargs: Dict[str, Any],
) -> None:
    """reset branch for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to reset.
        reset_type: string value of an enum containing "object", "common_prefix",
            "reset". defaults to "reset".
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        None

    Example:
        reset changes on a branch named `feature` for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import reset_branch

        @flow
        def reset_branch_for_example_repo():
            reset_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="feature",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.reset_branch,
            repository=repository,
            branch=branch,
            reset_creation=ResetCreation(type=reset_type),
            **lakefs_kwargs,
        )


@task
async def revert_branch(
    repository: str,
    branch: str,
    revert_ref: str,
    lakefs_credentials: LakeFSCredentials,
    parent_number: int = 1,
    **lakefs_kwargs: Dict[str, Any],
) -> None:
    """revert commit for branch for the lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to apply revert.
        revert_ref: string value of a ref to revert.
        parent_number: number representing parent index(based 1) to pick
            incase of reverting merge commits. Defaults to 1.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        None

    Example:
        revert changes on a branch named `feature` for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import revert_branch

        @flow
        def reset_branch_for_example_repo():
            revert_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="feature",
                revert_ref="exp_commit_sha",
            )
        ```
    """

    with lakefs_credentials.get_client("branches") as branches:
        return await run_sync_in_worker_thread(
            branches.revert_branch,
            repository=repository,
            branch=branch,
            revert_creation=RevertCreation(ref=revert_ref, parent_number=parent_number),
            **lakefs_kwargs,
        )
