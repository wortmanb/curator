"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
import warnings

from curator.actions.deepfreeze import PROVIDERS, SETTINGS_ID, STATUS_INDEX, Setup
from curator.actions.deepfreeze.rotate import Rotate
from curator.actions.deepfreeze.utilities import get_unmounted_repos
from curator.exceptions import ActionError, RepositoryException
from curator.s3client import s3_client_factory

from . import CuratorTestCase, random_suffix, testvars

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")
MET = "metadata"
INTERVAL = 1  # Because we can't go too fast or cloud providers can't keep up.


class TestCLISetup(CuratorTestCase):
    def test_rotate_happy_path(self):
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
            time.sleep(INTERVAL)

            rotate = Rotate(
                self.client,
            )
            # Perform the first rotation
            rotate.do_action()
            # There should now be two repositories.
            assert len(rotate.repo_list) == 2
            # Save off the current repo list
            orig_list = rotate.repo_list
            # Do another rotation with keep=1
            Rotate(
                self.client,
                keep=1,
            ).do_action()
            # There should again be two (one kept and one new)
            assert len(rotate.repo_list) == 2
            # They should not be the same two as before
            assert rotate.repo_list != orig_list
            # Query the settings index to get the unmountd repos
            unmounted = get_unmounted_repos(self.client)
            assert len(unmounted) == 1

            # Clean up
            s3.delete_bucket(testvars.df_bucket_name)
