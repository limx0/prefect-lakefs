import io

from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_lakefs.credentials import LakeFsCredentials


@task
async def get_object(
    repository: str,
    ref: str,
    key: str,
    credentials: LakeFsCredentials,
) -> bytes:
    client = credentials.objects_api
    stream = io.BytesIO()
    await run_sync_in_worker_thread(client.get_object, repository=repository, ref=ref, Key=key, Fileobj=stream)
    stream.seek(0)
    output = stream.read()
    return output


@task
async def upload(
    data: bytes,
    repository: str,
    ref: str,
    key: str,
    credentials: LakeFsCredentials,
) -> str:
    client = credentials.objects_api
    stream = io.BytesIO(data)
    await run_sync_in_worker_thread(client.upload_object, repository=repository, ref=ref, Key=key, Fileobj=stream)
    return key


@task
async def list_objects(
    repository: str,
    ref: str,
    prefix: str,
    credentials: LakeFsCredentials,
) -> list:
    client = credentials.objects_api
    objects = await run_sync_in_worker_thread(client.list_objects, repository=repository, ref=ref, prefix=prefix)
    return objects


@task
async def list_branches(
    repository: str,
    credentials: LakeFsCredentials,
) -> str:
    client = credentials.branches_api
    return client.list_branches(repository)
