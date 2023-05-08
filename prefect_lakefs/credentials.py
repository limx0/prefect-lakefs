from contextlib import contextmanager
from typing import Generator, Optional, Union

from lakefs_client import ApiClient
from lakefs_client.apis import BranchesApi, CommitsApi, ObjectsApi
from lakefs_client.client import LakeFSClient
from lakefs_client.configuration import Configuration
from prefect.blocks.core import Block
from prefect.utilities.collections import listrepr
from pydantic import Field, SecretStr
from typing_extensions import Literal

LAKEFS_CLIENT_TYPES = {
    "branches": BranchesApi,
    "commits": CommitsApi,
    "objects": ObjectsApi,
}


class LakeFSCredentials(Block):

    """Credentials block for generating configured LakeFS API clients.

    Attributes:
        access_key_id: s3 compatible access_key_id to initiate lakeFS client.
        secret_access_key: s3 compatible secret_access_key to initiate lakeFS client.
        endpoint_url: http/https url hosting the lakefs API.

    Example:
        Load stored LakeFS credentials:
        ```python
        from prefect_lakefs.credentials import LakeFSCredentials

        lakefs_credentials = LakeFSCredentials.load("BLOCK_NAME")
        ```
    """

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

    _block_type_name = "LakeFS Credentials"
    _block_type_slug = "lakefs-credentials"
    # TODO: add lakefs logo_url
    _logo_url = "https://styles.redditmedia.com/t5_57y5vj/styles/communityIcon_oapa1t4myyu71.png?width=256&v=enabled&s=d3319b91ef0a5eea78e46242e07f2034834f31f7"  # noqa
    # TODO: add LakeFSCredentials doc slug
    _documentation_url = "https://prefecthq.github.io"  # noqa
    _documentation_url = "https://limx0.github.io/prefect-lakefs/credentials/#prefect_lakefs.credentials.LakeFSCredentials"  # noqa

    @contextmanager
    def get_client(
        self,
        client_type: Literal["branches", "commits", "objects"],
    ) -> Generator[LakeFSClient, None, None]:
        """Convenience method for retrieving a LakeFS API client for deployment resources.

        Args:
            client_type: The resource-specific type of LakeFS client to retrieve.

        Yields:
            An authenticated, resource-specific LakeFS API client.

        Example:
            ```python
            from prefect_lakefs.credentials import LakeFSCredentials

            with LakeFSCredentials.get_client("branches") as branches:
                for branch in branches.list_branches(repository="example"):
                    print(branch.id)
            ```
        """

        client_config = Configuration(
            host=self.endpoint_url,
            username=self.access_key_id,
            password=self.secret_access_key.get_secret_value(),
        )
        with ApiClient(configuration=client_config) as generic_client:
            try:
                yield self.get_resource_specific_client(
                    client_type,
                    api_client=generic_client,
                )
            finally:
                generic_client.rest_client.pool_manager.clear()

    def get_resource_specific_client(
        self,
        client_type: str,
        api_client: ApiClient,
    ) -> Union[BranchesApi, CommitsApi, ObjectsApi]:
        """
        Utility function for configuring a generic LakeFS client.

        Args:
            client_type: The lakefs API client type for interacting with specific
                LakeFS resources.

        Returns:
            LakeFSClient: An authenticated, resource-specific LakeFS Client.

        Raises:
            ValueError: If `client_type` is not a valid LakeFS API client type.
        """

        try:
            return LAKEFS_CLIENT_TYPES[client_type](api_client=api_client)
        except KeyError:
            raise ValueError(
                f"Invalid client type provided '{client_type}'."
                f" Must be one of {listrepr(LAKEFS_CLIENT_TYPES.keys())}."
            )
