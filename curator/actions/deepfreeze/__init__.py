"""Deepfreeze Initialization"""

import refreeze
import rotate
import thaw

import setup

__all__ = ['setup', 'rotate', 'thaw', 'refreeze']

import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from elasticsearch8.exceptions import NotFoundError

from curator.exceptions import ActionError, RepositoryException

STATUS_INDEX = "deepfreeze-status"
SETTINGS_ID = "101"

def ensure_settings_index(client):
    """
    Ensure that the status index exists in Elasticsearch.

    :param client: A client connection object
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    if not client.indices.exists(index=STATUS_INDEX):
        loggit.info("Creating index %s", STATUS_INDEX)
        client.indices.create(index=STATUS_INDEX)


def get_settings(client):
    """
    Get the settings for the deepfreeze operation from the status index.

    :param client: A client connection object
    :returns: The settings
    :rtype: dict
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        doc = client.get(index=STATUS_INDEX, id=SETTINGS_ID)
        loggit.info("Settings document found")
        return Settings(doc["_source"])
    except client.exceptions.NotFoundError:
        loggit.info("Settings document not found")
        return None


def save_settings(client, settings):
    """
    Save the settings for the deepfreeze operation to the status index.

    :param client: A client connection object
    :param provider: The provider to use (AWS only for now)
    """
    #TODO: Add the ability to read and update the settings doc, if it already exists
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        existing_doc = client.get(index=STATUS_INDEX, id=SETTINGS_ID)
        loggit.info("Settings document already exists, updating it")
        client.update(index=STATUS_INDEX, id=SETTINGS_ID, doc=settings.__dict__)
    except NotFoundError:
        loggit.info("Settings document does not exist, creating it")
        client.create(index=STATUS_INDEX, id=SETTINGS_ID, document=settings.__dict__)
    loggit.info("Settings saved")


def create_new_bucket(bucket_name, dry_run=False):
    """
    Creates a new S3 bucket using the aws config in the environment.

    :param bucket_name: The name of the bucket to create
    :param dry_run: If True, do not actually create the bucket
    :returns:   whether the bucket was created or not
    :rtype:     bool
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Creating bucket %s", bucket_name)
    if dry_run:
        return
    try:
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        loggit.error(e)
        raise ActionError(e)


def create_new_repo(client, repo_name, bucket_name, base_path, canned_acl, 
                    storage_class, dry_run=False):
    """
    Creates a new repo using the previously-created bucket.

    :param client: A client connection object
    :param repo_name: The name of the repository to create
    :param bucket_name: The name of the bucket to use for the repository
    :param base_path_prefix: Path within a bucket where snapshots are stored
    :param canned_acl: One of the AWS canned ACL values
    :param storage_class: AWS Storage class
    :param dry_run: If True, do not actually create the repository
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    loggit.info("Creating repo %s using bucket %s", repo_name, bucket_name)
    if dry_run:
        return
    try:
        response = client.snapshot.create_repository(
            name=repo_name,
            body={
                "type": "s3",
                "settings": {
                    "bucket": bucket_name,
                    "base_path": base_path,
                    "canned_acl": canned_acl,
                    "storage_class": storage_class,
                }
            },
        )
    except Exception as e:
        loggit.error(e)
        raise ActionError(e)
    # TODO: Gather the reply and parse it to make sure this succeeded
    #       It should simply bring back '{ "acknowledged": true }' but I
    #       don't know how client will wrap it.
    loggit.info("Response: %s", response)


def get_next_suffix(style, last_suffix, year, month):
    """
    Gets the next suffix

    :param year: Optional year to override current year
    :param month: Optional month to override current month
    :returns: The next suffix in the format YYYY.MM
    :rtype: str
    """
    if style == "oneup":
        return str(int(last_suffix) + 1).zfill(6)
    else:
        current_year = year or datetime.now().year
        current_month = month or datetime.now().month
        return f"{current_year:04}.{current_month:02}"


def get_repos(client, repo_name_prefix):
    """
    Get the complete list of repos and return just the ones whose names
    begin with the given prefix.

    :param client: A client connection object
    :param repo_name_prefix: A prefix for repository names
    :returns: The repos.
    :rtype: list[object]
    """
    repos = client.snapshot.get_repository()
    pattern = re.compile(repo_name_prefix)
    logging.debug('Looking for repos matching %s', repo_name_prefix)
    return [repo for repo in repos if pattern.search(repo)]


def unmount_repo(client, repo, status_index):
    """
    Encapsulate the actions of deleting the repo and, at the same time,
    doing any record-keeping we need.

    :param client: A client connection object
    :param repo: The name of the repository to unmount
    :param status_index: The name of the status index
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    # repo_info = client.snapshot.get_repository(name=repo)
    # bucket = repo_info["settings"]["bucket"]
    # doc = {
    #     "repo": repo,
    #     "state": "deepfreeze",
    #     "timestamp": datetime.now().isoformat(),
    #     "bucket": bucket,
    #     "start": None,  # TODO: Add the earliest @timestamp value here
    #     "end": None,  # TODO: Add the latest @timestamp value here
    # }
    # client.create(index=status_index, document=doc)
    # Now that our records are complete, go ahead and remove the repo.
    client.snapshot.delete_repository(name=repo)
