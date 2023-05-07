from . import _version
from prefect_lakefs.credentials import LakeFSCredentials  # noqa F401

__version__ = _version.get_versions()["version"]
