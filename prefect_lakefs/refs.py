"""Module for interacting with LakeFS refs api from Prefect flows."""
from typing import Any, Dict

from lakefs_client.models import CommitList, DiffList, FindMergeBaseResult, MergeResult
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_lakefs.credentials import LakeFSCredentials


@task
async def diff_refs(
    repository: str,
    left_ref: str,
    right_ref: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> DiffList:
    """diff two refs of a lakefs repository.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        left_ref: a ref (could be commit id or branch).
        right_ref: a ref (could be commit id or branch).
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `DiffList` object.

    Example:
        diff `main` with `exp` branch for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import diff_refs

        @flow
        def diff_refs_main_exp_example_repo():
            difflist = diff_refs(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                left_ref="main",
                right_ref="exp",
            )
        ```
    """

    with lakefs_credentials.get_client("refs") as refs:
        return await run_sync_in_worker_thread(
            refs.diff_refs,
            repository=repository,
            left_ref=left_ref,
            right_ref=right_ref,
            **lakefs_kwargs,
        )


@task
async def find_merge_base(
    repository: str,
    source_ref: str,
    destination_branch: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> FindMergeBaseResult:
    """find the merge base given any 2 refs.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        source_ref: source reference.
        destination_branch: destination branch name.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `FindMergeBaseResult` object.

    Example:
        find merge base for refs main and exp for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import find_merge_base

        @flow
        def find_merge_base_main_exp_example_repo():
            merge_base = find_merge_base(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                source_ref="exp",
                destination_branch="main",
            )
        ```
    """

    with lakefs_credentials.get_client("refs") as refs:
        return await run_sync_in_worker_thread(
            refs.find_merge_base,
            repository=repository,
            source_ref=source_ref,
            destination_branch=destination_branch,
            **lakefs_kwargs,
        )


@task
async def log_commits(
    repository: str,
    ref: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> CommitList:
    """get commit log from ref. If both objects and prefixes are empty, return all commits. # noqa: E501

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: reference to fetch commit list.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `CommitList` object.

    Example:
        get commit list for ref main for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import log_commits

        @flow
        def log_commits_main_exp_example_repo():
            log = log_commits(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                ref="main",
            )
        ```
    """

    with lakefs_credentials.get_client("refs") as refs:
        return await run_sync_in_worker_thread(
            refs.log_commits,
            repository=repository,
            ref=ref,
            **lakefs_kwargs,
        )


@task
async def merge_into_branch(
    repository: str,
    source_ref: str,
    destination_branch: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> MergeResult:
    """merge references.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        source_ref: source reference.
        destination_branch: destination branch name to merge into.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `MergeResult` object.

    Example:
        merge exp branch into main for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import merge_into_branch

        @flow
        def merge_into_branch_exp_main_example_repo():
            merge_result = merge_into_branch(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                source_ref="exp",
                destination_branch="main",
            )
        ```
    """

    with lakefs_credentials.get_client("refs") as refs:
        return await run_sync_in_worker_thread(
            refs.merge_into_branch,
            repository=repository,
            source_ref=source_ref,
            destination_branch=destination_branch,
            **lakefs_kwargs,
        )
