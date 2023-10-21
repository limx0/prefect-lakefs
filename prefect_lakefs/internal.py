"""Module for interacting with LakeFS internal api from Prefect flows."""
from typing import Any, Dict

from lakefs_client.models import ObjectStageCreation, ObjectStats, RefsDump
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_lakefs.credentials import LakeFSCredentials


@task
async def dump_refs(
    repository: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> RefsDump:
    """Dump repository refs (tags, commits, branches) to object store.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `RefsDump` object.

    Example:
        dump refs for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import dump_refs

        @flow
        def dump_refs_example_repo():
            dump = dump_refs(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
            )
        ```
    """

    with lakefs_credentials.get_client("internal") as refs:
        return await run_sync_in_worker_thread(
            refs.dump_refs,
            repository=repository,
            **lakefs_kwargs,
        )


@task
async def stage_object(
    repository: str,
    ref: str,
    path: str,
    physical_address: str,
    checksum: str,
    size_bytes: int,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> ObjectStats:
    """stage object's metadata at given ref prefix.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: branch/ref name for which the objects to be staged for.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        ObjectStats

    Example:
        get object's existence at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import stage_object

        @flow
        def stage_object_for_main_example_repo():
            object_stat = await stage_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                ref="main",
                path="obj/path",
                physical_address="node_addr",
                checksum="checksum_hash",
                size_bytes=11,
            )
        ```
    """

    # TODO: should this be the expected behaviour? or fetch ObjectStats from elsewhere?
    with lakefs_credentials.get_client("internal") as objects:
        return await run_sync_in_worker_thread(
            objects.stage_object,
            repository=repository,
            ref=ref,
            path=path,
            object_stage_creation=ObjectStageCreation(
                physical_address=physical_address,
                checksum=checksum,
                size_bytes=size_bytes,
            ),
            **lakefs_kwargs,
        )


@task
async def restore_refs(
    repository: str,
    refs_dump: RefsDump,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> None:
    """Restore repository refs form object store.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        refs_dump: LakeFS RefsDump Model.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        None

    Example:
        restore refs dump for repository named `example`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import dump_refs
        from prefect_lakefs.tasks import restore_refs

        @flow
        def restore_refs_example_repo():
            dump = dump_refs(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
            )
            restore_refs(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                refs_dump=dump,
            )
        ```
    """

    with lakefs_credentials.get_client("internal") as refs:
        return await run_sync_in_worker_thread(
            refs.restore_refs,
            repository=repository,
            refs_dump=refs_dump,
            **lakefs_kwargs,
        )
