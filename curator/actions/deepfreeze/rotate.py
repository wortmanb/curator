# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import sys

from elasticsearch8 import Elasticsearch

from curator.actions.deepfreeze import (
    STATUS_INDEX,
    Repository,
    create_new_repo,
    ensure_settings_index,
    get_next_suffix,
    get_repos,
    get_settings,
    get_timestamp_range,
    save_settings,
    unmount_repo,
)
from curator.exceptions import RepositoryException
from curator.s3client import s3_client_factory


class Rotate:
    """
    The Deepfreeze is responsible for managing the repository rotation given
    a config file of user-managed options and settings.
    """

    def __init__(
        self,
        client: Elasticsearch,
        keep: str = "6",
        year: int = None,
        month: int = None,
    ) -> None:
        """
        :param client: A client connection object
        # :param repo_name_prefix: A prefix for repository names, defaults to `deepfreeze`
        # :param bucket_name_prefix: A prefix for bucket names, defaults to `deepfreeze`
        # :param base_path_prefix: Path within a bucket where snapshots are stored, defaults to `snapshots`
        # :param canned_acl: One of the AWS canned ACL values (see
        #     `<https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl>`),
        #     defaults to `private`
        # :param storage_class: AWS Storage class (see `<https://aws.amazon.com/s3/storage-classes/>`),
        #     defaults to `intelligent_tiering`
        :param keep: How many repositories to retain, defaults to 6
        :param year: Optional year to override current year
        :param month: Optional month to override current month
        """
        self.loggit = logging.getLogger("curator.actions.deepfreeze")
        self.loggit.debug("Initializing Deepfreeze Rotate")

        self.settings = get_settings(client)
        self.loggit.debug("Settings: %s", str(self.settings))

        self.client = client
        self.keep = int(keep)
        self.year = year
        self.month = month
        self.base_path = ""
        self.suffix = get_next_suffix(
            self.settings.style, self.settings.last_suffix, year, month
        )
        self.settings.last_suffix = self.suffix

        self.s3 = s3_client_factory(self.settings.provider)

        self.new_repo_name = f"{self.settings.repo_name_prefix}-{self.suffix}"
        if self.settings.rotate_by == "bucket":
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}-{self.suffix}"
            self.base_path = f"{self.settings.base_path_prefix}"
        else:
            self.new_bucket_name = f"{self.settings.bucket_name_prefix}"
            self.base_path = f"{self.settings.base_path_prefix}-{self.suffix}"

        self.loggit.debug("Getting repo list")
        self.repo_list = get_repos(self.client, self.settings.repo_name_prefix)
        self.repo_list.sort(reverse=True)
        self.loggit.debug("Repo list: %s", self.repo_list)
        self.latest_repo = ""
        try:
            self.latest_repo = self.repo_list[0]
            self.loggit.debug("Latest repo: %s", self.latest_repo)
        except IndexError:
            raise RepositoryException(
                f"no repositories match {self.settings.repo_name_prefix}"
            )
        if self.new_repo_name in self.repo_list:
            raise RepositoryException(f"repository {self.new_repo_name} already exists")
        if not self.client.indices.exists(index=STATUS_INDEX):
            self.client.indices.create(index=STATUS_INDEX)
            self.loggit.warning("Created index %s", STATUS_INDEX)
        self.loggit.info("Deepfreeze initialized")

    def update_ilm_policies(self, dry_run=False) -> None:
        """
        Loop through all existing IML policies looking for ones which reference
        the latest_repo and update them to use the new repo instead.
        """
        if self.latest_repo == self.new_repo_name:
            self.loggit.warning("Already on the latest repo")
            sys.exit(0)
        self.loggit.warning(
            "Switching from %s to %s", self.latest_repo, self.new_repo_name
        )
        policies = self.client.ilm.get_lifecycle()
        updated_policies = {}
        for policy in policies:
            # Go through these looking for any occurrences of self.latest_repo
            # and change those to use self.new_repo_name instead.
            # TODO: Ensure that delete_searchable_snapshot is set to false or
            # the snapshot will be deleted when the policy transitions to the next phase.
            # in this case, raise an error and skip this policy.
            # ? Maybe we don't correct this but flag it as an error?
            p = policies[policy]["policy"]["phases"]
            updated = False
            for phase in p:
                if "searchable_snapshot" in p[phase]["actions"] and (
                    p[phase]["actions"]["searchable_snapshot"]["snapshot_repository"]
                    == self.latest_repo
                ):
                    p[phase]["actions"]["searchable_snapshot"][
                        "snapshot_repository"
                    ] = self.new_repo_name
                    updated = True
            if updated:
                updated_policies[policy] = policies[policy]["policy"]

        # Now, submit the updated policies to _ilm/policy/<policyname>
        if not updated_policies:
            self.loggit.warning("No policies to update")
        else:
            self.loggit.info("Updating %d policies:", len(updated_policies.keys()))
        for pol, body in updated_policies.items():
            self.loggit.info("\t%s", pol)
            self.loggit.debug("Policy body: %s", body)
            if not dry_run:
                self.client.ilm.put_lifecycle(name=pol, policy=body)
            self.loggit.debug("Finished ILM Policy updates")

    def unmount_oldest_repos(self, dry_run=False) -> None:
        """
        Take the oldest repos from the list and remove them, only retaining
        the number chosen in the config under "keep".
        """
        # TODO: Look at snapshot.py for date-based calculations
        # Also, how to embed mutliple classes in a single action file
        # Alias action may be using multiple filter blocks. Look at that since we'll
        # need to do the same thing.:
        self.loggit.debug("Total list: %s", self.repo_list)
        s = self.repo_list[self.keep :]
        self.loggit.debug("Repos to remove: %s", s)
        for repo in s:
            self.loggit.info("Removing repo %s", repo)
            if not dry_run:
                unmount_repo(self.client, repo)

    def get_repo_details(self, repo: str) -> Repository:
        """
        Get all the relevant details about this repo and build a Repository object
        using them.

        Args:
            repo (str): Name of the repository

        Returns:
            Repository: A fleshed-out Repository object for persisting to ES.
        """
        response = self.client.get_repository(repo)
        earliest, latest = get_timestamp_range(self.client, [repo])
        return Repository(
            {
                "name": repo,
                "bucket": response["bucket"],
                "base_path": response["base_path"],
                "start": earliest,
                "end": latest,
                "is_mounted": False,
            }
        )

    def do_dry_run(self) -> None:
        """
        Perform a dry-run of the rotation process.
        """
        self.loggit.info("DRY-RUN MODE.  No changes will be made.")
        msg = (
            f"DRY-RUN: deepfreeze {self.latest_repo} will be rotated out"
            f" and {self.new_repo_name} will be added & made active."
        )
        self.loggit.info(msg)
        self.loggit.info("DRY-RUN: Creating bucket %s", self.new_bucket_name)
        create_new_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
            dry_run=True,
        )
        self.update_ilm_policies(dry_run=True)
        self.unmount_oldest_repos(dry_run=True)

    def do_action(self) -> None:
        """
        Perform high-level repo rotation steps in sequence.
        """
        ensure_settings_index(self.client)
        self.loggit.debug("Saving settings")
        save_settings(self.client, self.settings)
        self.s3.create_bucket(self.new_bucket_name)
        create_new_repo(
            self.client,
            self.new_repo_name,
            self.new_bucket_name,
            self.base_path,
            self.settings.canned_acl,
            self.settings.storage_class,
        )
        self.update_ilm_policies()
        self.unmount_oldest_repos()
