import io

from lakefs_client.exceptions import NotFoundException
from lakefs_client.model.commit_creation import CommitCreation
from prefect.context import FlowRunContext
from prefect_aws import S3Bucket
from pydantic import Field

from prefect_lakefs.credentials import LakeFSCredentials


class LakeFS(S3Bucket):
    _block_type_name = "LakeFS"

    credentials: LakeFSCredentials = Field(
        description="A block containing your LakeFS credentials."
    )

    def _get_s3_client(self):
        raise NotImplementedError(
            "Should be using LakeFS client via `self.credentials.get_client()`"
        )

    def _check_branch(self):
        with self.credentials.get_client("branches") as branches:
            try:
                branches.get_branch(
                    repository=self.credentials.repository,
                    branch=self.credentials.branch,
                )
                return
            except NotFoundException:
                branches.create_branch(
                    repository=self.credentials.repository,
                    branch_creation={
                        "name": self.credentials.branch,
                        "source": "main",
                    },
                )

    def _write_sync(self, key: str, data: bytes) -> None:
        self._check_branch()
        with self.credentials.get_client("objects") as client:
            with io.BytesIO(data) as stream:
                stream.name = key
                client.upload_object(
                    repository=self.credentials.repository,
                    branch=self.credentials.branch,
                    path=key,
                    content=stream,
                )
            if self.credentials.commit_on_task:
                self.commit_changes()

    def _read_sync(self, key: str) -> None:
        with self.credentials.get_client("objects") as client:
            data = client.get_object(
                repository=self.credentials.repository,
                ref=self.credentials.branch,
                path=key,
            )
            return data

    def commit_changes(self):
        repository = self.credentials.repository
        branch: str = self.credentials.branch
        assert branch, "If committing, `branch` must be set in `LakeFsCredentials`"

        with self.credentials.get_client("commits") as client:
            run_context: FlowRunContext = FlowRunContext.get()
            if run_context:
                metadata = {"flow_run_id": run_context.flow_run.id.hex}
            else:
                metadata = {}
            commit = CommitCreation("prefect-lakefs commit", metadata=metadata)
            client.commit(repository=repository, branch=branch, commit_creation=commit)
