import logging
from unittest.mock import patch

import boto3
import pytest
from botocore.stub import Stubber

from curator.exceptions import ActionError
from curator.s3client import AwsS3Client, AzureS3Client, GCPS3Client, s3_client_factory


@pytest.fixture
def aws_s3_client():
    """Fixture to create an AwsS3Client instance with a stubbed boto3 client."""
    client = boto3.client("s3")
    stubber = Stubber(client)
    aws_client = AwsS3Client()
    aws_client.client = client
    aws_client.loggit = logging.getLogger("AWS S3 Client")
    return aws_client, stubber


@pytest.fixture
def azure_s3_client():
    """Fixture to create an AzureS3Client instance with mocked environment variables."""
    with patch.dict(
        'os.environ',
        {
            "AZURE_S3_ENDPOINT_URL": "https://fake.azure.com",
            "AZURE_ACCESS_KEY": "fake_access_key",
            "AZURE_SECRET_KEY": "fake_secret_key",
        },
    ):
        client = boto3.client(
            's3',
            endpoint_url="https://fake.azure.com",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        stubber = Stubber(client)
        azure_client = AzureS3Client()
        azure_client.client = client
        azure_client.logger = logging.getLogger("Azure S3 Client")
        return azure_client, stubber


@pytest.fixture
def gcp_s3_client():
    """Fixture to create a GCPS3Client instance with mocked environment variables."""
    with patch.dict(
        'os.environ',
        {"GCP_ACCESS_KEY": "fake_access_key", "GCP_SECRET_KEY": "fake_secret_key"},
    ):
        client = boto3.client(
            's3',
            endpoint_url="https://storage.googleapis.com",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        stubber = Stubber(client)
        gcp_client = GCPS3Client()
        gcp_client.client = client
        gcp_client.logger = logging.getLogger("GCP S3 Client")
        return gcp_client, stubber


class TestS3Client:
    """Test suite for S3Client implementations."""

    def test_s3_client_factory_aws(self):
        """Test s3_client_factory creates AwsS3Client for 'aws' provider."""
        client = s3_client_factory("aws")
        assert isinstance(client, AwsS3Client)

    def test_s3_client_factory_azure(self):
        """Test s3_client_factory creates AzureS3Client for 'azure' provider."""
        with patch.dict(
            'os.environ',
            {
                "AZURE_S3_ENDPOINT_URL": "https://fake.azure.com",
                "AZURE_ACCESS_KEY": "fake_access_key",
                "AZURE_SECRET_KEY": "fake_secret_key",
            },
        ):
            client = s3_client_factory("azure")
            assert isinstance(client, AzureS3Client)

    def test_s3_client_factory_gcp(self):
        """Test s3_client_factory creates GCPS3Client for 'gcp' provider."""
        with patch.dict(
            'os.environ',
            {"GCP_ACCESS_KEY": "fake_access_key", "GCP_SECRET_KEY": "fake_secret_key"},
        ):
            client = s3_client_factory("gcp")
            assert isinstance(client, GCPS3Client)

    def test_s3_client_factory_invalid_provider(self):
        """Test s3_client_factory raises ValueError for invalid provider."""
        with pytest.raises(ValueError, match="Unsupported provider: invalid"):
            s3_client_factory("invalid")


class TestAwsS3Client:
    """Test suite for AwsS3Client."""

    def test_create_bucket_success(self, aws_s3_client):
        """Test creating a bucket successfully."""
        client, stubber = aws_s3_client
        # Stub head_bucket to indicate the bucket does not exist
        stubber.add_client_error(
            'head_bucket', service_error_code='404', service_message='Not Found'
        )
        # Stub create_bucket to simulate successful bucket creation
        stubber.add_response('create_bucket', {}, {'Bucket': 'test-bucket'})
        with stubber:
            client.create_bucket('test-bucket')
            stubber.assert_no_pending_responses()

    def test_create_bucket_already_exists(self, aws_s3_client):
        """Test creating a bucket that already exists raises ActionError."""
        client, stubber = aws_s3_client
        stubber.add_response('head_bucket', {}, {'Bucket': 'test-bucket'})
        with stubber:
            with pytest.raises(ActionError, match="Bucket test-bucket already exists"):
                client.create_bucket('test-bucket')

    def test_bucket_exists_true(self, aws_s3_client):
        """Test bucket_exists returns True for existing bucket."""
        client, stubber = aws_s3_client
        stubber.add_response('head_bucket', {}, {'Bucket': 'test-bucket'})
        with stubber:
            assert client.bucket_exists('test-bucket') is True

    def test_bucket_exists_false(self, aws_s3_client):
        """Test bucket_exists returns False for non-existing bucket."""
        client, stubber = aws_s3_client
        stubber.add_client_error(
            'head_bucket', service_error_code='404', service_message='Not Found'
        )
        with stubber:
            assert client.bucket_exists('test-bucket') is False

    def test_thaw_glacier_object(self, aws_s3_client):
        """Test thawing a Glacier object."""
        client, stubber = aws_s3_client
        stubber.add_response(
            'head_object',
            {'StorageClass': 'GLACIER'},
            {'Bucket': 'test-bucket', 'Key': 'test-key'},
        )
        stubber.add_response(
            'restore_object',
            {},
            {
                'Bucket': 'test-bucket',
                'Key': 'test-key',
                'RestoreRequest': {
                    'Days': 7,
                    'GlacierJobParameters': {'Tier': 'Standard'},
                },
            },
        )
        with stubber:
            client.thaw('test-bucket', '', ['test-key'])
            stubber.assert_no_pending_responses()

    def test_refreeze_object(self, aws_s3_client):
        """Test refreezing an object to Glacier."""
        client, stubber = aws_s3_client
        stubber.add_response(
            'list_objects_v2',
            {'Contents': [{'Key': 'test-key'}]},
            {'Bucket': 'test-bucket', 'Prefix': 'test-key'},
        )
        stubber.add_response(
            'copy_object',
            {},
            {
                'Bucket': 'test-bucket',
                'CopySource': {'Bucket': 'test-bucket', 'Key': 'test-key'},
                'Key': 'test-key',
                'StorageClass': 'GLACIER',
            },
        )
        with stubber:
            client.refreeze('test-bucket', 'test-key')
            stubber.assert_no_pending_responses()

    def test_list_objects(self, aws_s3_client):
        """Test listing objects in a bucket."""
        client, stubber = aws_s3_client
        stubber.add_response(
            'list_objects_v2',
            {'Contents': [{'Key': 'test-key1'}, {'Key': 'test-key2'}]},
            {'Bucket': 'test-bucket', 'Prefix': 'test/'},
        )
        with stubber:
            objects = client.list_objects('test-bucket', 'test/')
            assert len(objects) == 2
            assert objects[0]['Key'] == 'test-key1'
            assert objects[1]['Key'] == 'test-key2'

    def test_delete_bucket(self, aws_s3_client):
        """Test deleting a bucket."""
        client, stubber = aws_s3_client
        stubber.add_response('delete_bucket', {}, {'Bucket': 'test-bucket'})
        with stubber:
            client.delete_bucket('test-bucket')
            stubber.assert_no_pending_responses()

    def test_put_object(self, aws_s3_client):
        """Test putting an object in a bucket."""
        client, stubber = aws_s3_client
        stubber.add_response(
            'put_object',
            {},
            {'Bucket': 'test-bucket', 'Key': 'test-key', 'Body': 'test-body'},
        )
        with stubber:
            client.put_object('test-bucket', 'test-key', 'test-body')
            stubber.assert_no_pending_responses()

    def test_list_buckets(self, aws_s3_client):
        """Test listing all buckets."""
        client, stubber = aws_s3_client
        stubber.add_response(
            'list_buckets', {'Buckets': [{'Name': 'bucket1'}, {'Name': 'bucket2'}]}
        )
        with stubber:
            buckets = client.list_buckets(None)
            assert buckets == ['bucket1', 'bucket2']

    def test_copy_object(self, aws_s3_client):
        """Test copying an object."""
        client, stubber = aws_s3_client
        stubber.add_response(
            'copy_object',
            {},
            {
                'Bucket': 'dest-bucket',
                'CopySource': {'Bucket': 'source-bucket', 'Key': 'test-key'},
                'Key': 'new-key',
                'StorageClass': 'GLACIER',
            },
        )
        with stubber:
            client.copy_object(
                'dest-bucket', 'new-key', {'Bucket': 'source-bucket', 'Key': 'test-key'}
            )
            stubber.assert_no_pending_responses()


class TestAzureS3Client:
    """Test suite for AzureS3Client."""

    def test_init_missing_env_vars(self):
        """Test AzureS3Client initialization fails with missing environment variables."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing required environment variables"
            ):
                AzureS3Client()

    def test_create_bucket(self, azure_s3_client):
        """Test creating a bucket in Azure."""
        client, stubber = azure_s3_client
        stubber.add_response('create_bucket', {}, {'Bucket': 'test-bucket'})
        with stubber:
            client.create_bucket('test-bucket')
            stubber.assert_no_pending_responses()

    def test_thaw_not_supported(self, azure_s3_client, caplog):
        """Test thaw operation logs warning and skips."""
        client, stubber = azure_s3_client
        with caplog.at_level(logging.WARNING):
            client.thaw('test-bucket', 'test-path', ['test-key'])
            assert (
                "Azure Blob Storage does not support Glacier-like storage classes"
                in caplog.text
            )

    def test_refreeze_not_supported(self, azure_s3_client, caplog):
        """Test refreeze operation logs warning and skips."""
        client, stubber = azure_s3_client
        with caplog.at_level(logging.WARNING):
            client.refreeze('test-bucket', 'test-path')
            assert (
                "Azure Blob Storage does not support Glacier-like storage classes"
                in caplog.text
            )


class TestGCPS3Client:
    """Test suite for GCPS3Client."""

    def test_init_missing_env_vars(self):
        """Test GCPS3Client initialization fails with missing environment variables."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing required environment variables"
            ):
                GCPS3Client()

    def test_thaw_archive_object(self, gcp_s3_client):
        """Test thawing an ARCHIVE object in GCS."""
        client, stubber = gcp_s3_client
        stubber.add_response(
            'head_object',
            {'StorageClass': 'ARCHIVE'},
            {'Bucket': 'test-bucket', 'Key': 'test-key'},
        )
        stubber.add_response(
            'restore_object',
            {},
            {
                'Bucket': 'test-bucket',
                'Key': 'test-key',
                'RestoreRequest': {
                    'Days': 7,
                    'GlacierJobParameters': {'Tier': 'Standard'},
                },
            },
        )
        with stubber:
            client.thaw('test-bucket', '', ['test-key'])
            stubber.assert_no_pending_responses()

    def test_refreeze_object(self, gcp_s3_client):
        """Test refreezing an object to ARCHIVE in GCS."""
        client, stubber = gcp_s3_client
        stubber.add_response(
            'copy_object',
            {},
            {
                'Bucket': 'test-bucket',
                'CopySource': {'Bucket': 'test-bucket', 'Key': 'test-key'},
                'Key': 'test-key',
                'StorageClass': 'ARCHIVE',
                'MetadataDirective': 'COPY',
            },
        )
        with stubber:
            client.refreeze('test-bucket', 'test-key')
            stubber.assert_no_pending_responses()
