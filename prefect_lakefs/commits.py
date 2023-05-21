"""Module for interacting with LakeFS commits api from Prefect flows."""
from datetime import datetime
from typing import Any, Dict

from lakefs_client.models import Commit, CommitCreation, CommitList
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_lakefs.credentials import LakeFSCredentials


@task
async def commit(
    repository: str,
    branch: str,
    message: str,
    metadata: Dict[str, str],
    lakefs_credentials: LakeFSCredentials,
    date: int = int(datetime.now().timestamp()),
    **lakefs_kwargs: Dict[str, Any],
) -> Commit:
    """commit to a branch of lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: name of the branch to apply commit.
        message: commit message about the changes.
        metadata: additional metadata and context.
        date: epoch value of commit timestamp in seconds.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `Commit` object.

    Example:
        commit to branch named `main` for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import commit

        @flow
        def commit_to_main_example_repo():
            applied_commit = commit(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                message="commit message",
                metadata=dict(),
            )
        ```
    """

    with lakefs_credentials.get_client("commits") as commits:
        return await run_sync_in_worker_thread(
            commits.commit,
            repository=repository,
            branch=branch,
            commit_creation=CommitCreation(
                message=message, metadata=metadata, date=date
            ),
            **lakefs_kwargs,
        )


@task
async def get_commit(
    repository: str,
    commit_id: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> Commit:
    """fetch commit details of lakefs repository provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        commit_id: commit id to fetch details for.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `Commit` object.

    Example:
        fetch commit from repository named `example` given commit_id `commmit_sha`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import get_commit

        @flow
        def get_commit_for_example_repo():
            commit = get_commit(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                commit_id="commit_sha",
            )
        ```
    """

    with lakefs_credentials.get_client("commits") as commits:
        return await run_sync_in_worker_thread(
            commits.get_commit,
            repository=repository,
            commit_id=commit_id,
            **lakefs_kwargs,
        )


@task
async def log_branch_commits(
    repository: str,
    branch: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> CommitList:
    """get commit log for a lakefs branch provided.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: branch to fetch the commit log for.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `CommitList` object.

    Example:
        get commit log from repository named `example` given branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import log_branch_commits

        @flow
        def log_branch_commits_for_main_example_repo():
            commit = log_branch_commits(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
            )
        ```
    """

    with lakefs_credentials.get_client("commits") as commits:
        return await run_sync_in_worker_thread(
            commits.log_branch_commits,
            repository=repository,
            branch=branch,
            **lakefs_kwargs,
        )
