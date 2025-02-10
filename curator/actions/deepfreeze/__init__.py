import json
from dataclasses import dataclass
from datetime import datetime

from .refreeze import Refreeze
from .rotate import Rotate
from .setup import Setup
from .status import Status
from .thaw import Thaw
from .utilities import (
    create_new_repo,
    decode_date,
    ensure_settings_index,
    get_all_indices_in_repo,
    get_cluster_name,
    get_next_suffix,
    get_repos,
    get_repos_to_thaw,
    get_timestamp_range,
    get_unmounted_repos,
    print_centered_text,
    save_settings,
    thaw_repo,
    unmount_repo,
)

__all__ = [
    "Setup",
    "Rotate",
    "Refreeze",
    "Thaw",
    "Status",
    "Deepfreeze",
    "Settings",
    "Repository",
    "ThawSet",
    "ThawedRepo",
    "Deepfreeze",
    "create_new_repo",
    "decode_date",
    "ensure_settings_index",
    "get_all_indices_in_repo",
    "get_cluster_name",
    "get_next_suffix",
    "get_repos",
    "get_repos_to_thaw",
    "get_timestamp_range",
    "get_unmounted_repos",
    "print_centered_text",
    "save_settings",
    "thaw_repo",
    "unmount_repo",
    "STATUS_INDEX",
    "SETTINGS_ID",
]

STATUS_INDEX = "deepfreeze-status"
SETTINGS_ID = "1"


class Deepfreeze:
    """
    Allows nesting of actions under the deepfreeze command
    """


@dataclass
class ThawedRepo:
    """
    Data class for a thawed repo and indices
    """

    repo_name: str
    bucket_name: str
    base_path: str
    provider: str
    indices: list = None

    def __init__(self, repo_info: dict, indices: list[str] = None) -> None:
        self.repo_name = repo_info["name"]
        self.bucket_name = repo_info["bucket"]
        self.base_path = repo_info["base_path"]
        self.provider = "aws"
        self.indices = indices

    def add_index(self, index: str) -> None:
        """
        Add an index to the list of indices

        :param index: The index to add
        """
        self.indices.append(index)


class ThawSet(dict[str, ThawedRepo]):
    """
    Data class for thaw settings
    """

    def add(self, thawed_repo: ThawedRepo) -> None:
        """
        Add a thawed repo to the dictionary

        :param thawed_repo: A thawed repo object
        """
        self[thawed_repo.repo_name] = thawed_repo


@dataclass
class Repository:
    """
    Data class for repository
    """

    name: str
    bucket: str
    base_path: str
    start: datetime
    end: datetime
    is_thawed: bool = False
    is_mounted: bool = True
    doctype: str = "repository"

    def __init__(self, repo_hash=None) -> None:
        if repo_hash is not None:
            for key, value in repo_hash.items():
                setattr(self, key, value)

    def to_dict(self) -> dict:
        """
        Convert the Repository object to a dictionary.
        Convert datetime to ISO 8601 string format for JSON compatibility.
        """
        return {
            "name": self.name,
            "bucket": self.bucket,
            "base_path": self.base_path,
            "start": self.start.isoformat(),  # Convert datetime to string
            "end": self.end.isoformat(),  # Convert datetime to string
            "is_thawed": self.is_thawed,
            "is_mounted": self.is_mounted,
            "doctype": self.doctype,
        }

    def to_json(self) -> str:
        """
        Serialize the Repository object to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=4)


@dataclass
class Settings:
    """
    Data class for settings
    """

    doctype: str = "settings"
    repo_name_prefix: str = "deepfreeze"
    bucket_name_prefix: str = "deepfreeze"
    base_path_prefix: str = "snapshots"
    canned_acl: str = "private"
    storage_class: str = "intelligent_tiering"
    provider: str = "aws"
    rotate_by: str = "path"
    style: str = "oneup"
    last_suffix: str = None

    def __init__(self, settings_hash=None) -> None:
        if settings_hash is not None:
            for key, value in settings_hash.items():
                setattr(self, key, value)
