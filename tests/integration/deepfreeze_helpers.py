"""
Helper methods for deepfreeze integration tests

To avoid copying and pasting the same code over and over again, we can
put it here and import it where needed. Not every test will be able to use
these methods, but they can be useful for those that do.
"""

import time
import warnings

from elasticsearch import Elasticsearch

from curator.actions.deepfreeze.rotate import Rotate
from curator.actions.deepfreeze.setup import Setup
from curator.s3client import s3_client_factory
from tests.integration import random_suffix, testvars

INTERVAL = 1


def do_setup(
    client: Elasticsearch, provider: str, do_action=True, rotate_by: str = None
) -> Setup:
    """
    Perform a default setup for deepfreeze

    :param client: The Elasticsearch client
    :type client: Elasticsearch
    :param provider: Which cloud S3 provider to use
    :type provider: str
    :param do_action: Whether to perform the setup action or not, defaults to True
    :type do_action: bool, optional
    :param rotate_by: Rotate by bucket or path within a bucket, defaults to `path`
    :type rotate_by: str, optional

    :return: The Setup object
    :rtype: Setup
    """
    warnings.filterwarnings(
        "ignore", category=DeprecationWarning, module="botocore.auth"
    )
    s3 = s3_client_factory(provider)
    testvars.df_bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

    if rotate_by:
        testvars.df_rotate_by = rotate_by

    setup = Setup(
        client,
        bucket_name_prefix=testvars.df_bucket_name,
        repo_name_prefix=testvars.df_repo_name,
        base_path_prefix=testvars.df_base_path,
        storage_class=testvars.df_storage_class,
        rotate_by=testvars.df_rotate_by,
        style=testvars.df_style,
    )
    if do_action:
        setup.do_action()
        time.sleep(INTERVAL)
    return setup


def do_rotate(
    client: Elasticsearch, iterations: int = 1, populate_index=False
) -> Rotate:
    """
    Helper method to perform a number of rotations

    :param client: The Elasticsearch client
    :type client: Elasticsearch
    :param populate_index: Whether to populate the index before rotating, defaults to False
    :type populate_index: bool, optional
    :param iterations: How many iterations to perform, defaults to 1
    :type iterations: int, optional

    :return: The Rotate object
    :rtype: Rotate
    """
    rotate = None
    for _ in range(iterations):
        rotate = Rotate(
            client,
        )
        if populate_index:
            _populate_index(client, testvars.test_index)
        rotate.do_action()
        time.sleep(INTERVAL)
    return rotate


def _populate_index(client: Elasticsearch, index: str, doc_count: int = 1000) -> None:
    """
    Populate an index with a given number of documents

    :param client: The Elasticsearch client
    :type client: Elasticsearch
    :param index: The index to populate
    :type index: str
    :param doc_count: The number of documents to create, defaults to 1000
    :type doc_count: int, optional

    :return: None
    :rtype: None
    """
    for _ in range(doc_count):
        client.index(index=index, body={"foo": "bar"})
