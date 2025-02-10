# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
from datetime import datetime

from elasticsearch8 import Elasticsearch

from curator.s3client import s3_client_factory

from .thawset import ThawedRepo, ThawSet
from .utilities import decode_date, get_settings, thaw_repo


class Thaw:
    """
    Thaw a deepfreeze repository
    """

    def __init__(
        self,
        client: Elasticsearch,
        start: datetime,
        end: datetime,
        retain: int,
        storage_class: str,
        enable_multiple_buckets: bool = False,
    ) -> None:
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.start = decode_date(start)
        self.end = decode_date(end)
        self.retain = retain
        self.storage_class = storage_class
        self.enable_multiple_buckets = enable_multiple_buckets

        self.s3 = s3_client_factory(self.settings.provider)

    def do_action(self) -> None:
        """
        Perform high-level repo thawing steps in sequence.
        """
        # We don't save the settings here because nothing should change our settings.
        # What we _will_ do though, is save a ThawSet showing what indices and repos
        # were thawed out.

        thawset = ThawSet()

        for repo in self.get_repos_to_thaw():
            self.loggit.info("Thawing %s", repo)
            if self.provider == "aws":
                if self.setttings.rotate_by == "bucket":
                    bucket = f"{self.settings.bucket_name_prefix}-{self.settings.last_suffix}"
                    path = self.settings.base_path_prefix
                else:
                    bucket = f"{self.settings.bucket_name_prefix}"
                    path = (
                        f"{self.settings.base_path_prefix}-{self.settings.last_suffix}"
                    )
            else:
                raise ValueError("Invalid provider")
            thaw_repo(self.s3, bucket, path, self.retain, self.storage_class)
            repo_info = self.client.get_repository(repo)
            thawset.add(ThawedRepo(repo_info))
