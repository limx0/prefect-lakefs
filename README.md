# prefect_lakefs

~~A prefect collection for interacting with [LakeFS](https://lakefs.io).~~

This repo is an experimental _work in progress_, exploring what an integration between Prefect and LakeFS could look
like.

## Install

Install via pip (source-only at this stage.)

`$ pip install git+https://github.com/limx0/prefect-lakefs`

# Getting Started

## Credentials

`prefect-lakefs` uses a `CredentialsBlock` for credentials, very similar to an AWS S3 Credentials block, with
additional LakeFS specific fields (repository, branch).

```python
from prefect_lakefs import LakeFsCredentials

credentials = LakeFsCredentials(
    repository="my_repo",
    access_key_id="ACCESS_KEY",
    secret_access_key="SECRET_KEY",
    branch="my_branch"
)

```

See [prefect-aws#saving-credentials-to-a-block](https://github.com/PrefectHQ/prefect-aws#saving-credentials-to-a-block)
for an example of saving credentials to a block.

## Result Storage

Write task results to LakeFS as you would any other block (see https://docs.prefect.io/concepts/results/#result-storage-location)

```python
from prefect import flow, task
from prefect_lakefs import LakeFsCredentials, LakeFS

@task(persist_result=True)
def gets_data():
    # result from this task will be saved to LakeFS
    ...

@flow(result_storage=LakeFS(credentials=LakeFsCredentials.load("LAKE_FS_CREDENTIALS"), repository="my_repo"))
def myflow():
    # some tasks
    ...

```

## Tasks

Individual tasks for interacting with LakeFS are found in [tasks](prefect_lakefs/tasks.py)

```python
from prefect import flow
from prefect_lakefs import LakeFsCredentials
from prefect_lakefs.tasks import get_object

@flow
def lake_fs_flow():
    credentials = LakeFsCredentials.load("LAKE_FS_CREDENTIALS")
    obj = get_object(
        repository="my_repo",
        ref="main",
        key="/path/to/data",
        credentials=credentials
    )
    return obj

```
