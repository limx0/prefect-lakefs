import pandas as pd
from prefect import flow, task

from prefect_lakefs import LakeFSCredentials
from prefect_lakefs.storage import LakeFS


@task
def extract():
    return pd.read_parquet("lakes.parquet")


@task(
    result_storage=LakeFS(
        bucket_name="lakefs",
        credentials=LakeFSCredentials(
            access_key_id="",
            secret_access_key="",
            repository="dev",
            branch="main",
            commit_on_task=True,
        ),
    ),
    persist_result=True,
)
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
