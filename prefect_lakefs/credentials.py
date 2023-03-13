from typing import Optional

from lakefs_client.api.branches_api import BranchesApi
from lakefs_client.api.commits_api import CommitsApi
from lakefs_client.api.objects_api import ObjectsApi
from lakefs_client.client import LakeFSClient
from lakefs_client.configuration import Configuration
from prefect.blocks.abstract import CredentialsBlock
from pydantic import Field
from pydantic import SecretStr


class LakeFsCredentials(CredentialsBlock):
    repository: str = Field(
        description="The LakeFS repository to use.",
        title="The LakeFS repository.",
    )

    access_key_id: Optional[str] = Field(
        default=None,
        description="A specific LakeFS access key ID.",
        title="LakeFS Access Key ID",
    )
    secret_access_key: Optional[SecretStr] = Field(
        default=None,
        description="A specific LakeFS secret access key.",
        title="LakeFS Access Key Secret",
    )
    endpoint_url: Optional[str] = Field(
        default="http://localhost:8000/api/v1",
        description="A LakeFS URL.",
        title="LakeFS URL",
    )

    branch: Optional[str] = Field(
        default=None,
        description="The LakeFS branch to use.",
        title="LakeFS branch",
    )
    commit_on_task: bool = Field(
        default=False,
        description="Whether to commit every completed task.",
        title="Commit on completed Task",
    )
    commit_on_flow: bool = Field(
        default=False,
        description="Whether to commit every completed flow.",
        title="Commit on completed Flow",
    )
    mirror_git_repo: bool = Field(
        default=False,
        description="Whether to include metadata from local git repo in LakeFS commit metadata",
        title="Mirrow Git Repo",
    )
    strict_no_dirty: bool = Field(
        default=False,
        description="Whether to ensure all local git changes have been committed (no un-staged changes / dirty repo)",
        title="Strict no dirty git repo",
    )

    @property
    def client(self) -> LakeFSClient:
        """
        Gets a LakeFS client.

        Returns:
            LakeFSClient.
        """
        username = self.access_key_id
        password = self.secret_access_key
        configuration = Configuration(
            host=self.endpoint_url,
            username=username,
            password=password,
        )
        client: LakeFSClient = LakeFSClient(configuration)
        return client

    @property
    def objects_api(self) -> ObjectsApi:
        objects: ObjectsApi = self.client.objects
        return objects

    @property
    def commits_api(self) -> CommitsApi:
        commits: CommitsApi = self.client.commits
        return commits

    @property
    def branches_api(self) -> BranchesApi:
        branches: CommitsApi = self.client.branches
        return branches
