# pylint: disable=too-many-arguments,too-many-instance-attributes, raise-missing-from

import logging
import re
from datetime import datetime

from elasticsearch8 import Elasticsearch
from elasticsearch8.exceptions import NotFoundError
from rich import print
from rich.align import Align
from rich.console import Console

from curator.exceptions import ActionError
from curator.s3client import S3Client

from .constants import SETTINGS_ID, STATUS_INDEX
from .repository import Repository
from .settings import Settings

#
#
# Utility functions
#
#


def get_cluster_name(client: Elasticsearch) -> str:
    """
    Connects to the Elasticsearch cluster and returns its name.

    :param es_host: The URL of the Elasticsearch instance (default: "http://localhost:9200").
    :return: The name of the Elasticsearch cluster.
    """
    try:
        cluster_info = client.cluster.health()
        return cluster_info.get("cluster_name", "Unknown Cluster")
    except Exception as e:
        return f"Error: {e}"


def thaw_repo(
    s3: S3Client,
    bucket_name: str,
    base_path: str,
    restore_days: int = 7,
    retrieval_tier: str = "Standard",
) -> None:
    """
    Thaw a repository in Elasticsearch

    :param client: A client connection object
    :param bucket_name: The name of the bucket
    :param object_key: The key of the object
    :param restore_days: Number of days to keep the object accessible
    :param retrieval_tier: 'Standard' or 'Expedited' or 'Bulk'

    :raises: NotFoundError

    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=base_path)

    # Check if objects were found
    if "Contents" not in response:
        print(f"No objects found in prefix: {base_path}")
        return

    # Loop through each object and initiate restore for Glacier objects
    count = 0
    for obj in response["Contents"]:
        object_key = obj["Key"]

        # Initiate the restore request for each object
        logging.debug("Initiating restore request for %s", object_key)
        count += 1
        s3.restore_object(
            Bucket=bucket_name,
            Key=object_key,
            RestoreRequest={
                "Days": restore_days,
                "GlacierJobParameters": {
                    "Tier": retrieval_tier  # You can change to 'Expedited' or 'Bulk' if needed
                },
            },
        )

    print(f"Requested restore of {count} objects")


def get_all_indices_in_repo(client: Elasticsearch, repository: str) -> list[str]:
    """
    Retrieve all indices from snapshots in the given repository.

    :param client: A client connection object
    :param repository: The name of the repository
    :returns: A list of indices
    :rtype: list[str]
    """
    snapshots = client.snapshot.get(repository=repository, snapshot="_all")
    indices = set()

    for snapshot in snapshots["snapshots"]:
        indices.update(snapshot["indices"])

    logging.debug("Indices: %s", indices)

    return list(indices)


def get_timestamp_range(
    client: Elasticsearch, indices: list[str]
) -> tuple[datetime, datetime]:
    """
    Retrieve the earliest and latest @timestamp values from the given indices.

    :param client: A client connection object
    :param indices: A list of indices
    :returns: A tuple containing the earliest and latest @timestamp values
    :rtype: tuple[datetime, datetime]
    """
    if not indices:
        return None, None

    query = {
        "size": 0,
        "aggs": {
            "earliest": {"min": {"field": "@timestamp"}},
            "latest": {"max": {"field": "@timestamp"}},
        },
    }

    response = client.search(index=",".join(indices), body=query)

    earliest = response["aggregations"]["earliest"]["value_as_string"]
    latest = response["aggregations"]["latest"]["value_as_string"]

    logging.debug("Earliest: %s, Latest: %s", earliest, latest)

    return datetime.fromisoformat(earliest), datetime.fromisoformat(latest)


def ensure_settings_index(client: Elasticsearch) -> None:
    """
    Ensure that the status index exists in Elasticsearch.

    :param client: A client connection object
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    if not client.indices.exists(index=STATUS_INDEX):
        loggit.info("Creating index %s", STATUS_INDEX)
        client.indices.create(index=STATUS_INDEX)


def get_settings(client: Elasticsearch) -> Settings:
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
    except NotFoundError:
        loggit.info("Settings document not found")
        return None


def get_repos_to_thaw(
    client: Elasticsearch, start: datetime, end: datetime
) -> list[Repository]:
    """
    Get the list of repos that were active during the given time range.

    :param client: A client connection object
    :param start: The start of the time range
    :param end: The end of the time range
    :returns: The repos
    :rtype: list[Repository] A list of repository names
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    repos = get_unmounted_repos(client)
    overlapping_repos = []
    for repo in repos:
        if repo.start <= end and repo.end >= start:
            overlapping_repos.append(repo)
    loggit.info("Found overlapping repos: %s", overlapping_repos)
    return overlapping_repos


def save_settings(client: Elasticsearch, settings: Settings) -> None:
    """
    Save the settings for the deepfreeze operation to the status index.

    :param client: A client connection object
    :param provider: The provider to use (AWS only for now)
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    try:
        client.get(index=STATUS_INDEX, id=SETTINGS_ID)
        loggit.info("Settings document already exists, updating it")
        client.update(index=STATUS_INDEX, id=SETTINGS_ID, doc=settings.__dict__)
    except NotFoundError:
        loggit.info("Settings document does not exist, creating it")
        client.create(index=STATUS_INDEX, id=SETTINGS_ID, document=settings.__dict__)
    loggit.info("Settings saved")


def create_new_repo(
    client,
    repo_name: str,
    bucket_name: str,
    base_path: str,
    canned_acl: str,
    storage_class: str,
    dry_run: bool = False,
) -> None:
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
                },
            },
        )
    except Exception as e:
        loggit.error(e)
        print(
            f"[magenta]Error creating repository. Ensure AWS credentials have been added to keystore:[/magenta] {e}"
        )
        raise ActionError(e)
    #
    # TODO: Gather the reply and parse it to make sure this succeeded
    #       It should simply bring back '{ "acknowledged": true }' but I
    #       don't know how client will wrap it.
    loggit.info("Response: %s", response)


def get_next_suffix(style: str, last_suffix: str, year: int, month: int) -> str:
    """
    Gets the next suffix

    :param year: Optional year to override current year
    :param month: Optional month to override current month
    :returns: The next suffix in the format YYYY.MM
    :rtype: str
    """
    if style == "oneup":
        return str(int(last_suffix) + 1).zfill(6)
    elif style == "date":
        current_year = year or datetime.now().year
        current_month = month or datetime.now().month
        return f"{current_year:04}.{current_month:02}"
    else:
        raise ValueError("Invalid style")


def get_unmounted_repos(client: Elasticsearch) -> list[Repository]:
    """
    Get the complete list of repos from our index and return a Repository object for each.

    :param client: A client connection object
    :returns: The unmounted repos.
    :rtype: list[Repository]
    """
    # logging.debug("Looking for unmounted repos")
    # # Perform search in ES for all repos in the status index
    query = {"query": {"match": {"doctype": "repository"}}}
    response = client.search(index=STATUS_INDEX, body=query)
    repos = response["hits"]["hits"]
    # return a Repository object for each
    return [Repository(repo["_source"]) for repo in repos]


def get_repos(client: Elasticsearch, repo_name_prefix: str) -> list[str]:
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
    logging.debug("Looking for repos matching %s", repo_name_prefix)
    return [repo for repo in repos if pattern.search(repo)]


def unmount_repo(client: Elasticsearch, repo: str) -> None:
    """
    Encapsulate the actions of deleting the repo and, at the same time,
    doing any record-keeping we need.

    :param client: A client connection object
    :param repo: The name of the repository to unmount
    :param status_index: The name of the status index
    """
    loggit = logging.getLogger("curator.actions.deepfreeze")
    repo_info = client.snapshot.get_repository(name=repo)[repo]
    bucket = repo_info["settings"]["bucket"]
    base_path = repo_info["settings"]["base_path"]
    earliest, latest = get_timestamp_range(
        client, get_all_indices_in_repo(client, repo)
    )
    repodoc = Repository(
        {
            "name": repo,
            "bucket": bucket,
            "base_path": base_path,
            "is_mounted": False,
            "start": decode_date(earliest),
            "end": decode_date(latest),
            "doctype": "repository",
        }
    )
    msg = f"Recording repository details as {repodoc}"
    loggit.debug(msg)
    client.index(index=STATUS_INDEX, document=repodoc.to_dict())
    loggit.debug("Removing repo %s", repo)
    # Now that our records are complete, go ahead and remove the repo.
    client.snapshot.delete_repository(name=repo)


def decode_date(date_in: str) -> datetime:
    if isinstance(date_in, datetime):
        return date_in
    elif isinstance(date_in, str):
        return datetime.fromisoformat(date_in)
    else:
        return datetime.now()  # FIXME: This should be a value error
        # raise ValueError("Invalid date format")


def print_centered_text(text):
    console = Console()
    centered_text = Align.center(text)
    console.print(centered_text)
