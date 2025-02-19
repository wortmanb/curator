"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import warnings

from curator.actions.deepfreeze import PROVIDERS, SETTINGS_ID, STATUS_INDEX, Setup
from curator.s3client import s3_client_factory

from . import CuratorTestCase, testvars

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")
MET = "metadata"


class TestCLISetup(CuratorTestCase):
    def test_setup(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )
        for provider in PROVIDERS:
            s3 = s3_client_factory(provider)

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

            # Cleanup
            self.delete_repositories()
            s3.delete_bucket(testvars.df_bucket_name)

    # def test_setup_bucket_exists(self):
    #     pass

    # def test_setup_path_exists(self):
    #     pass

    # def test_setup_repo_exists(self):
    #     pass

    # def test_setup_bucket_path_repo_exist(self):
    #     pass

    # def test_setup_status_index_exists(self):
    #     pass
