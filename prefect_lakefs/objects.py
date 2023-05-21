"""Module for interacting with LakeFS objects api from Prefect flows."""
from typing import Any, Dict, List

from lakefs_client.models import (
    ObjectCopyCreation,
    ObjectErrorList,
    ObjectStageCreation,
    ObjectStats,
    ObjectStatsList,
    PathList,
    UnderlyingObjectProperties,
)
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_lakefs.credentials import LakeFSCredentials


@task
async def copy_object(
    repository: str,
    branch: str,
    dest_path: str,
    src_path: str,
    lakefs_credentials: LakeFSCredentials,
    src_ref: str = "",
    **lakefs_kwargs: Dict[str, Any],
) -> ObjectStats:
    """create a copy of an object.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: branch name which the object belongs to.
        dest_path: destination path to copy the object to.(relative to branch/ref)
        src_path: source path to copy the object from.(relative to branch/ref)
        src_ref: ref the source object belongs to. defaults to branch if not specified.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        A LakeFS `ObjectStats` object.

    Example:
        create a copy of object at `src/path` to `dest/path` in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import copy_object

        @flow
        def copy_object_for_main_example_repo():
            object_stat = copy_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                dest_path="dest/path",
                src_path="src/path",
            )
        ```
    """

    # TODO: handle object error when raised.
    if src_ref == "":
        src_ref = branch

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.copy_object,
            repository=repository,
            branch=branch,
            dest_path=dest_path,
            object_copy_creation=ObjectCopyCreation(src_path=src_path, src_ref=src_ref),
            **lakefs_kwargs,
        )


@task
async def delete_object(
    repository: str,
    branch: str,
    path: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> None:
    """delete an object given a path relative to branch/ref.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: branch name which the object belongs to.
        path: path of the object to delete.(relative to branch/ref)
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        None

    Example:
        delete object at `obj/path` in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import delete_object

        @flow
        def delete_object_for_main_example_repo():
            delete_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                path="obj/path",
            )
        ```
    """

    # TODO: handle error for missing objects.

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.delete_object,
            repository=repository,
            branch=branch,
            path=path,
            **lakefs_kwargs,
        )


@task
async def delete_objects(
    repository: str,
    branch: str,
    path_list: List[str],
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> ObjectErrorList:
    """delete an object given a list of paths relative to branch/ref.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: branch name which the object belongs to.
        path_list: list of strings which are paths of the objects to be deleted.
            (relative to branch/ref)
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        ObjectErrorList

    Example:
        delete objects at given list of paths in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import delete_objects

        @flow
        def delete_objects_for_main_example_repo():
            delete_objects(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                path_list=["obj/path"],
            )
        ```
    """

    # TODO: handle ObjectErrorList.
    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.delete_objects,
            repository=repository,
            branch=branch,
            path_list=PathList(paths=path_list),
            **lakefs_kwargs,
        )


@task
async def get_object(
    repository: str,
    ref: str,
    path: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
):
    """get an object at a path relative to branch/ref.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: branch/ref name which the object belongs to.
        path: object path.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        blob

    Example:
        get object at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import get_object

        @flow
        def get_object_for_main_example_repo():
            blob = get_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                path="obj/path",
            )
        ```
    """

    # TODO: check return type bytes.
    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.get_object,
            repository=repository,
            ref=ref,
            path=path,
            **lakefs_kwargs,
        )


@task
async def get_underlying_properties(
    repository: str,
    ref: str,
    path: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> UnderlyingObjectProperties:
    """get an object's storage class properties at a path relative to branch/ref.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: branch/ref name which the object belongs to.
        path: object path.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        UnderlyingObjectProperties

    Example:
        get object's storage metadata at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import get_underlying_properties

        @flow
        def get_underlying_properties_for_main_example_repo():
            props = await get_underlying_properties(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                path="obj/path",
            )
        ```
    """

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.get_underlying_properties,
            repository=repository,
            ref=ref,
            path=path,
            **lakefs_kwargs,
        )


@task
async def head_object(
    repository: str,
    ref: str,
    path: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> None:
    """get object's existence at a path relative to branch/ref.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: branch/ref name which the object belongs to.
        path: object path.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        None

    Example:
        get object's existence at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import head_object

        @flow
        def head_object_for_main_example_repo():
            await head_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                path="obj/path",
            )
        ```
    """

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.head_object,
            repository=repository,
            ref=ref,
            path=path,
            **lakefs_kwargs,
        )


@task
async def list_objects(
    repository: str,
    ref: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> ObjectStatsList:
    """list objects' metadata at given ref prefix.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: branch/ref name for which the objects to be listed for.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        ObjectStatsList

    Example:
        get object's existence at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import list_objects

        @flow
        def list_objects_for_main_example_repo():
            object_stat_list = await list_objects(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
            )
        ```
    """

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.list_objects,
            repository=repository,
            ref=ref,
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
    with lakefs_credentials.get_client("objects") as objects:
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
async def stat_object(
    repository: str,
    ref: str,
    path: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> ObjectStats:
    """fetch object's metadata at given ref prefix.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        ref: branch/ref name for which the objects to be staged for.
        path: path of the object to fetch metadata for.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        ObjectStats

    Example:
        get object's existence at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import stat_object

        @flow
        def stat_object_for_main_example_repo():
            object_stat = await stage_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                ref="main",
                path="obj/path",
            )
        ```
    """

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.stat_object,
            repository=repository,
            ref=ref,
            path=path,
            **lakefs_kwargs,
        )


@task
async def upload_object(
    repository: str,
    branch: str,
    path: str,
    lakefs_credentials: LakeFSCredentials,
    **lakefs_kwargs: Dict[str, Any],
) -> ObjectStats:
    """upload object at given path for ref prefix.

    Args:
        lakefs_credentials: `LakeFSCredentials` block for creating
            authenticated LakeFS API clients.
        repository: name of a lakefs repository.
        branch: branch/ref name for which the objects to be staged for.
        path: path of the object to fetch metadata for.
        **lakefs_kwargs: Optional extra keyword arguments to pass to the LakeFS API.

    Returns:
        ObjectStats

    Example:
        get object's existence at given path in branch `main`:

        ```python
        from prefect import flow
        from prefect_lakefs import LakeFSCredentials
        from prefect_lakefs.tasks import upload_object

        @flow
        def upload_object_for_main_example_repo():
            object_stat = await upload_object(
                lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
                repository="example",
                branch="main",
                path="obj/path",
            )
        ```
    """
    # TODO: provide example with file handle.

    with lakefs_credentials.get_client("objects") as objects:
        return await run_sync_in_worker_thread(
            objects.upload_object,
            repository=repository,
            branch=branch,
            path=path,
            **lakefs_kwargs,
        )
