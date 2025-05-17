import logging
import os
import time
from typing import Dict, List, Union

import pytest
from botocore.client import BaseClient

from curator.s3client import s3_client_factory


# Fixture to generate unique bucket names
@pytest.fixture
def unique_bucket_name():
    """Generate a unique bucket name with a timestamp suffix."""
    timestamp = int(time.time())
    return f"test-bucket-{timestamp}"


# Fixture to set up environment variables for Azure
@pytest.fixture
def azure_env_vars():
    """Set up environment variables for Azure S3 client."""
    required_vars = {
        "AZURE_S3_ENDPOINT_URL": os.getenv("AZURE_S3_ENDPOINT_URL"),
        "AZURE_ACCESS_KEY": os.getenv("AZURE_ACCESS_KEY"),
        "AZURE_SECRET_KEY": os.getenv("AZURE_SECRET_KEY"),
    }
    if not all(required_vars.values()):
        pytest.skip("Missing Azure environment variables")
    return required_vars


# Fixture to set up environment variables for GCP
@pytest.fixture
def gcp_env_vars():
    """Set up environment variables for GCP S3 client."""
    required_vars = {
        "GCP_ACCESS_KEY": os.getenv("GCP_ACCESS_KEY"),
        "GCP_SECRET_KEY": os.getenv("GCP_SECRET_KEY"),
    }
    if not all(required_vars.values()):
        pytest.skip("Missing GCP environment variables")
    return required_vars


class TestS3ClientIntegration:
    """Integration tests for S3Client implementations against real APIs."""

    @pytest.mark.parametrize("provider", ["aws", "azure", "gcp"])
    def test_get_client(self, provider, azure_env_vars, gcp_env_vars):
        """Test get_client returns a BaseClient instance for each provider."""
        client = s3_client_factory(provider)
        boto3_client = client.get_client()
        assert isinstance(
            boto3_client, BaseClient
        ), f"Expected BaseClient for {provider} provider"

    @pytest.mark.parametrize("provider", ["aws", "azure", "gcp"])
    def test_create_and_delete_bucket(
        self, provider, unique_bucket_name, azure_env_vars, gcp_env_vars
    ):
        """Test creating and deleting a bucket for each provider."""
        client = s3_client_factory(provider)

        # Create bucket
        client.create_bucket(unique_bucket_name)

        # Verify bucket exists
        assert client.bucket_exists(
            unique_bucket_name
        ), f"Bucket {unique_bucket_name} should exist"

        # Clean up: Delete objects and bucket
        objects: Union[List[str], List[Dict]] = client.list_objects(
            unique_bucket_name, ""
        )
        for obj in objects:
            key = obj['Key'] if provider == "aws" and isinstance(obj, dict) else obj
            client.get_client().delete_object(Bucket=unique_bucket_name, Key=key)
        client.delete_bucket(unique_bucket_name)

        # Verify bucket no longer exists
        assert not client.bucket_exists(
            unique_bucket_name
        ), f"Bucket {unique_bucket_name} should be deleted"

    @pytest.mark.parametrize("provider", ["aws", "azure", "gcp"])
    def test_put_and_list_object(
        self, provider, unique_bucket_name, azure_env_vars, gcp_env_vars
    ):
        """Test putting an object and listing it for each provider."""
        client = s3_client_factory(provider)

        # Create bucket
        client.create_bucket(unique_bucket_name)

        # Put object
        test_key = "test-object.txt"
        test_body = "Hello, world!"
        client.put_object(unique_bucket_name, test_key, test_body)

        # List objects
        objects: Union[List[str], List[Dict]] = client.list_objects(
            unique_bucket_name, ""
        )
        if provider == "aws":
            object_keys = [obj['Key'] for obj in objects if isinstance(obj, dict)]
        else:
            object_keys = objects
        assert (
            test_key in object_keys
        ), f"Object {test_key} should be in bucket {unique_bucket_name}"

        # Clean up: Delete object and bucket
        client.get_client().delete_object(Bucket=unique_bucket_name, Key=test_key)
        client.delete_bucket(unique_bucket_name)

    @pytest.mark.parametrize("provider", ["aws", "azure", "gcp"])
    def test_copy_object(
        self, provider, unique_bucket_name, azure_env_vars, gcp_env_vars
    ):
        """Test copying an object within a bucket for each provider."""
        client = s3_client_factory(provider)

        # Create bucket
        client.create_bucket(unique_bucket_name)

        # Put source object
        source_key = "source-object.txt"
        client.put_object(unique_bucket_name, source_key, "Source content")

        # Copy object
        dest_key = "dest-object.txt"
        copy_source = {"Bucket": unique_bucket_name, "Key": source_key}
        storage_class = (
            "STANDARD"
            if provider == "azure"
            else "GLACIER" if provider == "aws" else "ARCHIVE"
        )
        client.copy_object(
            unique_bucket_name, dest_key, copy_source, storage_class=storage_class
        )

        # Verify copied object exists
        objects: Union[List[str], List[Dict]] = client.list_objects(
            unique_bucket_name, ""
        )
        if provider == "aws":
            object_keys = [obj['Key'] for obj in objects if isinstance(obj, dict)]
        else:
            object_keys = objects
        assert (
            dest_key in object_keys
        ), f"Copied object {dest_key} should be in bucket {unique_bucket_name}"

        # Clean up: Delete objects and bucket
        client.get_client().delete_object(Bucket=unique_bucket_name, Key=source_key)
        client.get_client().delete_object(Bucket=unique_bucket_name, Key=dest_key)
        client.delete_bucket(unique_bucket_name)

    def test_aws_thaw_and_refreeze(self, unique_bucket_name):
        """Test thawing and refreezing an object in AWS S3 (Glacier)."""
        client = s3_client_factory("aws")

        # Create bucket
        client.create_bucket(unique_bucket_name)

        # Put object and move to Glacier
        test_key = "glacier-object.txt"
        client.put_object(unique_bucket_name, test_key, "Test content")
        client.copy_object(
            unique_bucket_name,
            test_key,
            {"Bucket": unique_bucket_name, "Key": test_key},
            storage_class="GLACIER",
        )

        # Thaw object
        client.thaw(unique_bucket_name, "", [test_key], restore_days=1)

        # Note: Verifying thaw completion requires waiting (can take hours), so we skip verification

        # Refreeze object
        client.refreeze(unique_bucket_name, test_key, storage_class="GLACIER")

        # Clean up: Delete object and bucket
        client.get_client().delete_object(Bucket=unique_bucket_name, Key=test_key)
        client.delete_bucket(unique_bucket_name)

    def test_gcp_thaw_and_refreeze(self, unique_bucket_name, gcp_env_vars):
        """Test thawing and refreezing an object in GCP (Archive)."""
        client = s3_client_factory("gcp")

        # Create bucket
        client.create_bucket(unique_bucket_name)

        # Put object and move to Archive
        test_key = "archive-object.txt"
        client.put_object(unique_bucket_name, test_key, "Test content")
        client.copy_object(
            unique_bucket_name,
            test_key,
            {"Bucket": unique_bucket_name, "Key": test_key},
            storage_class="ARCHIVE",
        )

        # Thaw object
        client.thaw(unique_bucket_name, "", [test_key], restore_days=1)

        # Note: Verifying thaw completion requires waiting (can take hours), so we skip verification

        # Refreeze object
        client.refreeze(unique_bucket_name, test_key, storage_class="ARCHIVE")

        # Clean up: Delete object and bucket
        client.get_client().delete_object(Bucket=unique_bucket_name, Key=test_key)
        client.delete_bucket(unique_bucket_name)

    def test_azure_thaw_refreeze_skipped(
        self, unique_bucket_name, azure_env_vars, caplog
    ):
        """Test that thaw and refreeze operations are skipped in Azure with appropriate logging."""
        client = s3_client_factory("azure")

        # Create bucket
        client.create_bucket(unique_bucket_name)

        # Test thaw (should log warning and skip)
        with caplog.at_level(logging.WARNING):
            client.thaw(unique_bucket_name, "test-path", ["test-key"])
            assert (
                "Azure Blob Storage does not support Glacier-like storage classes"
                in caplog.text
            )

        # Test refreeze (should log warning and skip)
        with caplog.at_level(logging.WARNING):
            client.refreeze(unique_bucket_name, "test-path")
            assert (
                "Azure Blob Storage does not support Glacier-like storage classes"
                in caplog.text
            )

        # Clean up: Delete bucket
        client.delete_bucket(unique_bucket_name)
