"""
Test deepfreeze setup functionality
"""

# pylint: disable=missing-function-docstring, missing-class-docstring, line-too-long
import os
import time
import warnings

from curator.actions.deepfreeze import PROVIDERS
from curator.actions.deepfreeze.rotate import Rotate
from curator.actions.deepfreeze.utilities import get_unmounted_repos
from curator.s3client import s3_client_factory
from tests.integration.deepfreeze_helpers import do_setup

from . import CuratorTestCase, random_suffix, testvars

HOST = os.environ.get("TEST_ES_SERVER", "http://127.0.0.1:9200")
MET = "metadata"


class TestDeepfreezeRotate(CuratorTestCase):
    def test_rotate_happy_path(self):
        warnings.filterwarnings(
            "ignore", category=DeprecationWarning, module="botocore.auth"
        )
        for provider in PROVIDERS:
            testvars.df_bucket_name = f"{testvars.df_bucket_name}-{random_suffix()}"
            do_setup(provider)
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
            s3 = s3_client_factory(provider)
            s3.delete_bucket(testvars.df_bucket_name)
