# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import json
from dataclasses import dataclass
from datetime import datetime


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
