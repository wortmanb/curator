"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
import warnings

from curator.actions.deepfreeze import PROVIDERS, SETTINGS_ID, STATUS_INDEX, Setup
from curator.exceptions import ActionError, RepositoryException
from curator.s3client import s3_client_factory

from . import CuratorTestCase, random_suffix, testvars

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")
MET = "metadata"
INTERVAL = 1  # Because we can't go too fast or cloud providers can't keep up.


class TestCLISetup(CuratorTestCase):
    def test_setup(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )
        for provider in PROVIDERS:
            s3 = s3_client_factory(provider)
            testvars.df_bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

            setup = Setup(
                self.client,
                bucket_name_prefix=testvars.df_bucket_name,
                repo_name_prefix=testvars.df_repo_name,
                base_path_prefix=testvars.df_base_path,
                storage_class=testvars.df_storage_class,
                rotate_by=testvars.df_rotate_by,
                style=testvars.df_style,
            )
            setup.do_action()
            # Don't ask me why this is necessary, but the test has a tendency to fail
            # without it.
            time.sleep(INTERVAL)
            csi = self.client.cluster.state(metric=MET)[MET]["indices"]

            # Specific assertions
            # Settings index should exist
            assert csi[STATUS_INDEX]
            # Settings doc should exist within index
            assert self.client.get(index=STATUS_INDEX, id=SETTINGS_ID)
            # Settings index should only have settings doc (count == 1)
            assert 1 == self.client.count(index=STATUS_INDEX)["count"]
            # Repo should exist
            assert self.client.snapshot.get_repository(
                name=f"{testvars.df_repo_name}-000001"
            )
            # Bucket should exist
            assert s3.bucket_exists(testvars.df_bucket_name)
            # We can't test the base path on AWS because it won't be created until the
            #  first object is written, but we can test the settings to see if it's correct
            #  there.
            s = self.get_settings()
            assert s.base_path_prefix == testvars.df_base_path
            assert s.last_suffix == "000001"
            assert s.canned_acl == testvars.df_acl
            assert s.storage_class == testvars.df_storage_class
            assert s.provider == "aws"
            assert s.rotate_by == testvars.df_rotate_by
            assert s.style == testvars.df_style
            assert s.repo_name_prefix == testvars.df_repo_name
            assert s.bucket_name_prefix == testvars.df_bucket_name

            # Clean up
            self.client.snapshot.delete_repository(
                name=f"{testvars.df_repo_name}-000001"
            )
            self.client.indices.delete(index=STATUS_INDEX)
            s3.delete_bucket(testvars.df_bucket_name)

    def test_setup_bucket_exists(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )
        s3 = s3_client_factory('aws')
        testvars.df_bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"

        # Pre-create the bucket to simlulate picking a bucket that already exists.
        s3.create_bucket(testvars.df_bucket_name)
        time.sleep(INTERVAL)

        setup = Setup(
            self.client,
            bucket_name_prefix=testvars.df_bucket_name,
            repo_name_prefix=testvars.df_repo_name,
            base_path_prefix=testvars.df_base_path,
            storage_class=testvars.df_storage_class,
            rotate_by=testvars.df_rotate_by,
            style=testvars.df_style,
        )
        with self.assertRaises(ActionError):
            setup.do_action()
        # Clean up
        self.client.indices.delete(index=STATUS_INDEX)
        s3.delete_bucket(testvars.df_bucket_name)

    def test_setup_repo_exists(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )
        for provider in PROVIDERS:
            s3 = s3_client_factory(provider)
            testvars.df_bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"
            testvars.df_bucket_name_2 = f"{testvars.df_bucket_name_2}-{random_suffix()}"

            # Pre-create the bucket and repo to simulate picking a repo that already \
            # exists. We use a different bucket name to avoid the bucket already exists
            # error.
            s3.create_bucket(testvars.df_bucket_name_2)
            time.sleep(INTERVAL)
            self.client.snapshot.create_repository(
                name=f"{testvars.df_repo_name}-000001",
                body={
                    "type": "s3",
                    "settings": {
                        "bucket": testvars.df_bucket_name_2,
                        "base_path": testvars.df_base_path_2,
                        "storage_class": testvars.df_storage_class,
                    },
                },
            )

            with self.assertRaises(RepositoryException):
                setup = Setup(
                    self.client,
                    bucket_name_prefix=testvars.df_bucket_name,
                    repo_name_prefix=testvars.df_repo_name,
                    base_path_prefix=testvars.df_base_path,
                    storage_class=testvars.df_storage_class,
                    rotate_by=testvars.df_rotate_by,
                    style=testvars.df_style,
                )
                setup.do_action()

            # Clean up
            self.client.snapshot.delete_repository(
                name=f"{testvars.df_repo_name}-000001"
            )
            s3.delete_bucket(testvars.df_bucket_name_2)
