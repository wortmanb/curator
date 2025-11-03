"""Test GCP Storage Client"""

from unittest.mock import MagicMock, patch, call
import pytest
from curator.exceptions import ActionError
from curator.s3client import GcpStorageClient, s3_client_factory, HAS_GCP


@pytest.mark.skipif(not HAS_GCP, reason="GCP dependencies not installed")
class TestGcpStorageClient:
    """Test GcpStorageClient class"""

    def setup_method(self):
        """Setup for each test"""
        with patch('curator.s3client.storage.Client'):
            self.gcp = GcpStorageClient(location="us-west1")
            self.gcp.client = MagicMock()

    def test_init(self):
        """Test GcpStorageClient initialization"""
        with patch('curator.s3client.storage.Client') as mock_storage:
            mock_client = MagicMock()
            mock_client.list_buckets.return_value = iter([])
            mock_storage.return_value = mock_client

            gcp = GcpStorageClient(location="us-central1")

            mock_storage.assert_called_once()
            assert gcp.loggit is not None
            assert gcp.location == "us-central1"
            # Verify credential validation call
            mock_client.list_buckets.assert_called_once_with(max_results=1)

    def test_init_invalid_credentials(self):
        """Test GcpStorageClient initialization with invalid credentials"""
        with patch('curator.s3client.storage.Client') as mock_storage:
            from google.auth import exceptions as gauth_exceptions

            mock_client = MagicMock()
            mock_client.list_buckets.side_effect = (
                gauth_exceptions.DefaultCredentialsError("No credentials")
            )
            mock_storage.return_value = mock_client

            with pytest.raises(ActionError, match="GCP credentials are invalid"):
                GcpStorageClient()

    def test_test_connection_success(self):
        """Test successful connection test"""
        self.gcp.client.list_buckets.return_value = iter([])
        assert self.gcp.test_connection() is True

    def test_test_connection_failure(self):
        """Test failed connection test"""
        self.gcp.client.list_buckets.side_effect = Exception("Network error")
        assert self.gcp.test_connection() is False

    def test_create_bucket_success(self):
        """Test successful bucket creation"""
        mock_bucket = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        self.gcp.bucket_exists = MagicMock(return_value=False)

        self.gcp.create_bucket("test-bucket")

        self.gcp.client.bucket.assert_called_with("test-bucket")
        assert mock_bucket.location == "us-west1"
        self.gcp.client.create_bucket.assert_called_with(
            mock_bucket, location="us-west1"
        )

    def test_create_bucket_already_exists(self):
        """Test bucket creation when bucket already exists"""
        self.gcp.bucket_exists = MagicMock(return_value=True)
        with pytest.raises(ActionError, match="already exists"):
            self.gcp.create_bucket("test-bucket")

    def test_create_bucket_error(self):
        """Test bucket creation with error"""
        mock_bucket = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        self.gcp.bucket_exists = MagicMock(return_value=False)
        self.gcp.client.create_bucket.side_effect = Exception("API Error")

        with pytest.raises(ActionError):
            self.gcp.create_bucket("test-bucket")

    def test_bucket_exists_true(self):
        """Test bucket_exists returns True when bucket exists"""
        mock_bucket = MagicMock()
        mock_bucket.exists.return_value = True
        self.gcp.client.bucket.return_value = mock_bucket

        assert self.gcp.bucket_exists("test-bucket") is True
        self.gcp.client.bucket.assert_called_with("test-bucket")

    def test_bucket_exists_false(self):
        """Test bucket_exists returns False when bucket doesn't exist"""
        mock_bucket = MagicMock()
        mock_bucket.exists.return_value = False
        self.gcp.client.bucket.return_value = mock_bucket

        assert self.gcp.bucket_exists("test-bucket") is False

    def test_thaw_archive_objects(self):
        """Test thawing objects from Archive with dict metadata"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        object_keys = [
            {"Key": "base_path/file1", "StorageClass": "ARCHIVE"},
            {"Key": "base_path/file2", "StorageClass": "ARCHIVE"},
        ]

        self.gcp.thaw("test-bucket", "base_path", object_keys, 7, "Standard")

        assert mock_blob.update_storage_class.call_count == 2
        mock_blob.update_storage_class.assert_called_with("STANDARD")

    def test_thaw_coldline_objects(self):
        """Test thawing objects from Coldline"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        object_keys = [{"Key": "base_path/file1", "StorageClass": "COLDLINE"}]

        self.gcp.thaw("test-bucket", "base_path", object_keys, 7, "Standard")

        mock_blob.update_storage_class.assert_called_once_with("STANDARD")

    def test_thaw_skip_non_archive(self):
        """Test thaw skips non-Archive/Coldline storage classes"""
        mock_bucket = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket

        object_keys = [{"Key": "base_path/file1", "StorageClass": "STANDARD"}]

        self.gcp.thaw("test-bucket", "base_path", object_keys, 7, "Standard")

        # Should not call update_storage_class for STANDARD objects
        assert not mock_bucket.blob.called

    def test_thaw_skip_wrong_path(self):
        """Test thaw skips objects outside base_path"""
        mock_bucket = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket

        object_keys = [{"Key": "wrong_path/file1", "StorageClass": "ARCHIVE"}]

        self.gcp.thaw("test-bucket", "base_path", object_keys, 7, "Standard")

        # Should not process objects outside base_path
        assert not mock_bucket.blob.called

    def test_refreeze_success(self):
        """Test successful refreezing of objects"""
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = "base_path/file1"
        mock_blob1.storage_class = "STANDARD"
        mock_blob2 = MagicMock()
        mock_blob2.name = "base_path/file2"
        mock_blob2.storage_class = "STANDARD"

        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = iter([mock_blob1, mock_blob2])

        self.gcp.refreeze("test-bucket", "base_path", "ARCHIVE")

        mock_bucket.list_blobs.assert_called_once_with(prefix="base_path")
        assert mock_blob1.update_storage_class.call_count == 1
        assert mock_blob2.update_storage_class.call_count == 1
        mock_blob1.update_storage_class.assert_called_with("ARCHIVE")
        mock_blob2.update_storage_class.assert_called_with("ARCHIVE")

    def test_refreeze_to_coldline(self):
        """Test refreezing to Coldline"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.name = "base_path/file1"
        mock_blob.storage_class = "STANDARD"

        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = iter([mock_blob])

        self.gcp.refreeze("test-bucket", "base_path", "COLDLINE")

        mock_blob.update_storage_class.assert_called_with("COLDLINE")

    def test_list_objects_success(self):
        """Test successful listing of objects"""
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = "file1"
        mock_blob1.size = 100
        mock_blob1.storage_class = "STANDARD"
        mock_blob1.updated = "2025-01-01"

        mock_blob2 = MagicMock()
        mock_blob2.name = "file2"
        mock_blob2.size = 200
        mock_blob2.storage_class = "ARCHIVE"
        mock_blob2.updated = "2025-01-02"

        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = iter([mock_blob1, mock_blob2])

        result = self.gcp.list_objects("test-bucket", "prefix")

        assert len(result) == 2
        assert result[0]["Key"] == "file1"
        assert result[0]["Size"] == 100
        assert result[0]["StorageClass"] == "STANDARD"
        assert result[1]["Key"] == "file2"
        assert result[1]["StorageClass"] == "ARCHIVE"

    def test_list_objects_empty(self):
        """Test listing objects when no objects exist"""
        mock_bucket = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = iter([])

        result = self.gcp.list_objects("test-bucket", "prefix")
        assert result == []

    def test_delete_bucket_success(self):
        """Test successful bucket deletion"""
        mock_bucket = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket

        self.gcp.delete_bucket("test-bucket")

        self.gcp.client.bucket.assert_called_with("test-bucket")
        mock_bucket.delete.assert_called_once()

    def test_delete_bucket_with_force(self):
        """Test bucket deletion with force=True empties bucket first"""
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = "file1.txt"
        mock_blob2 = MagicMock()
        mock_blob2.name = "file2.txt"

        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = iter([mock_blob1, mock_blob2])

        self.gcp.delete_bucket("test-bucket", force=True)

        # Should list and delete objects
        mock_bucket.list_blobs.assert_called_once()
        mock_blob1.delete.assert_called_once()
        mock_blob2.delete.assert_called_once()

        # Should delete bucket
        mock_bucket.delete.assert_called_once()

    def test_delete_bucket_error(self):
        """Test bucket deletion error"""
        mock_bucket = MagicMock()
        mock_bucket.delete.side_effect = Exception("Bucket not empty")
        self.gcp.client.bucket.return_value = mock_bucket

        with pytest.raises(ActionError):
            self.gcp.delete_bucket("test-bucket")

    def test_put_object_success(self):
        """Test successful object put"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        self.gcp.put_object("test-bucket", "key", "body content")

        self.gcp.client.bucket.assert_called_with("test-bucket")
        mock_bucket.blob.assert_called_with("key")
        mock_blob.upload_from_string.assert_called_with("body content")

    def test_put_object_empty_body(self):
        """Test putting object with empty body"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        self.gcp.put_object("test-bucket", "key")

        mock_blob.upload_from_string.assert_called_with("")

    def test_put_object_error(self):
        """Test put object error"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.upload_from_string.side_effect = Exception("Upload failed")
        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        with pytest.raises(ActionError):
            self.gcp.put_object("test-bucket", "key", "body")

    def test_list_buckets_success(self):
        """Test successful bucket listing"""
        mock_bucket1 = MagicMock()
        mock_bucket1.name = "bucket1"
        mock_bucket2 = MagicMock()
        mock_bucket2.name = "bucket2"
        mock_bucket3 = MagicMock()
        mock_bucket3.name = "test-bucket3"

        self.gcp.client.list_buckets.return_value = iter(
            [mock_bucket1, mock_bucket2, mock_bucket3]
        )

        result = self.gcp.list_buckets()
        assert result == ["bucket1", "bucket2", "test-bucket3"]

    def test_list_buckets_with_prefix(self):
        """Test bucket listing with prefix filter"""
        mock_bucket1 = MagicMock()
        mock_bucket1.name = "bucket1"
        mock_bucket2 = MagicMock()
        mock_bucket2.name = "test-bucket2"
        mock_bucket3 = MagicMock()
        mock_bucket3.name = "test-bucket3"

        self.gcp.client.list_buckets.return_value = iter(
            [mock_bucket1, mock_bucket2, mock_bucket3]
        )

        result = self.gcp.list_buckets(prefix="test-")
        assert result == ["test-bucket2", "test-bucket3"]

    def test_list_buckets_empty(self):
        """Test listing buckets when none exist"""
        self.gcp.client.list_buckets.return_value = iter([])

        result = self.gcp.list_buckets()
        assert result == []

    def test_list_buckets_error(self):
        """Test bucket listing error"""
        self.gcp.client.list_buckets.side_effect = Exception("Access denied")

        with pytest.raises(ActionError):
            self.gcp.list_buckets()

    def test_head_object_success(self):
        """Test successful head object retrieval"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.size = 1024
        mock_blob.storage_class = "ARCHIVE"
        mock_blob.updated = "2025-01-01"
        mock_blob.etag = "abc123"
        mock_blob.content_type = "text/plain"

        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        result = self.gcp.head_object("test-bucket", "test-key")

        assert result["ContentLength"] == 1024
        assert result["StorageClass"] == "ARCHIVE"
        assert result["ETag"] == "abc123"
        assert result["ContentType"] == "text/plain"
        mock_blob.reload.assert_called_once()

    def test_head_object_error(self):
        """Test head object error"""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.reload.side_effect = Exception("Not found")

        self.gcp.client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        with pytest.raises(ActionError, match="Error getting metadata"):
            self.gcp.head_object("test-bucket", "test-key")

    def test_copy_object_success(self):
        """Test successful object copy"""
        mock_source_bucket = MagicMock()
        mock_source_blob = MagicMock()
        mock_dest_bucket = MagicMock()
        mock_dest_blob = MagicMock()

        def bucket_side_effect(name):
            if name == "src-bucket":
                return mock_source_bucket
            else:
                return mock_dest_bucket

        self.gcp.client.bucket.side_effect = bucket_side_effect
        mock_source_bucket.blob.return_value = mock_source_blob
        mock_dest_bucket.blob.return_value = mock_dest_blob

        # Mock rewrite to complete in one iteration
        mock_dest_blob.rewrite.return_value = (None, 1024, 1024)

        self.gcp.copy_object(
            Bucket="dest-bucket",
            Key="dest-key",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
            StorageClass="COLDLINE",
        )

        mock_dest_blob.rewrite.assert_called_once()
        mock_dest_blob.update_storage_class.assert_called_with("COLDLINE")

    def test_copy_object_default_storage_class(self):
        """Test object copy with default storage class"""
        mock_source_bucket = MagicMock()
        mock_source_blob = MagicMock()
        mock_dest_bucket = MagicMock()
        mock_dest_blob = MagicMock()

        def bucket_side_effect(name):
            if name == "src-bucket":
                return mock_source_bucket
            else:
                return mock_dest_bucket

        self.gcp.client.bucket.side_effect = bucket_side_effect
        mock_source_bucket.blob.return_value = mock_source_blob
        mock_dest_bucket.blob.return_value = mock_dest_blob

        # Mock rewrite to complete in one iteration
        mock_dest_blob.rewrite.return_value = (None, 1024, 1024)

        self.gcp.copy_object(
            Bucket="dest-bucket",
            Key="dest-key",
            CopySource={"Bucket": "src-bucket", "Key": "src-key"},
        )

        # Should use default ARCHIVE
        mock_dest_blob.update_storage_class.assert_called_with("ARCHIVE")


class TestS3ClientFactoryGcp:
    """Test s3_client_factory function for GCP"""

    @pytest.mark.skipif(not HAS_GCP, reason="GCP dependencies not installed")
    def test_factory_gcp(self):
        """Test factory returns GcpStorageClient for gcp provider"""
        with patch('curator.s3client.storage.Client'):
            client = s3_client_factory("gcp")
            assert isinstance(client, GcpStorageClient)

    @pytest.mark.skipif(HAS_GCP, reason="Test only runs without GCP installed")
    def test_factory_gcp_missing_dependency(self):
        """Test factory raises ActionError when GCP dependency is missing"""
        with pytest.raises(
            ActionError,
            match="Install with: pip install elasticsearch-curator\\[gcp\\]",
        ):
            s3_client_factory("gcp")
