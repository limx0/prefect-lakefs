import io

from lakefs_client.model.commit_creation import CommitCreation
from prefect.context import FlowRunContext
from prefect_aws import S3Bucket
from pydantic import Field

from prefect_lakefs.credentials import LakeFSCredentials


class LakeFS(S3Bucket):
    _block_type_name = "LakeFS"

    credentials: LakeFSCredentials = Field(
        description="A block containing your credentials to AWS or MinIO.",
    )

    def _get_s3_client(self):
        # return self.credentials.get_client()
        raise NotImplementedError(
            "Should be using LakeFS client via `self.credentials.get_client()`"
        )

    def _write_sync(self, key: str, data: bytes) -> None:
        with self.credentials.get_client("objects") as client:
            with io.BytesIO(data) as stream:
                stream.name = key
                client.upload_object(
                    repository=self.credentials.repository,
                    branch=self.credentials.branch,
                    path=key,
                    content=stream,
                )
        # if self.credentials.commit_on_task:
        #     self.commit_changes()

    def _read_sync(self, key: str) -> None:
        client = self.credentials.objects_api
        data = client.get_object(
            repository=self.credentials.repository,
            ref=self._get_branch(),
            path=key,
        )
        return data

    def commit_changes(self):
        repository = self.credentials.repository
        branch: str = self.credentials.branch
        assert branch, "If committing, `branch` must be set in `LakeFsCredentials`"
        commits_api = self.credentials.commits_api
        run_context: FlowRunContext = FlowRunContext.get()
        metadata = {
            "flow_run_id": run_context.flow.id,
        }
        commit = CommitCreation("prefect-lakefs commit", metadata=metadata)
        commits_api.commit(repository=repository, branch=branch, commit_creation=commit)
