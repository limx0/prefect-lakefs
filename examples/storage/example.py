from typing import Optional

import pandas as pd
import yaml  # type: ignore
from git import InvalidGitRepositoryError, NoSuchPathError, Repo
from prefect import flow, task

from prefect_lakefs import LakeFSCredentials
from prefect_lakefs.storage import LakeFS


def read_creds():
    data = yaml.safe_load(open("lakectl.yaml").read())
    return {
        "access_key_id": data["credentials"]["access_key_id"],
        "secret_access_key": data["credentials"]["secret_access_key"],
    }


def get_git_repo() -> Optional[Repo]:
    try:
        return Repo(".", search_parent_directories=True)
    except (InvalidGitRepositoryError, NoSuchPathError):
        return None


def get_branch_name() -> Optional[str]:
    repo = get_git_repo()
    if repo is None:
        return None
    return repo.active_branch.name


def create_lakefs_storage() -> LakeFS:
    """
    Create a LakeFS result storage for prefect.

    Link this projects git branches to lakefs branches.

    """
    return LakeFS(
        bucket_name="lakefs",
        credentials=LakeFSCredentials(
            **read_creds(),
            repository="lakefs",
            branch=get_branch_name() or "main",
            commit_on_task=True,
        ),
    )


@task
def extract():
    return pd.read_parquet("lakes.parquet")


@task(result_storage=create_lakefs_storage(), persist_result=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df


@task
def load(df: pd.DataFrame) -> None:
    print(df)


@flow
def etl():
    e = extract()
    t = transform(e)
    load(t)


if __name__ == "__main__":
    etl()
