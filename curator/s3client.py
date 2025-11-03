"""
s3client.py

import boto3

Encapsulate the S3 client here so it can be used by all Curator classes, not just
deepfreeze.
"""

import abc
import logging

import boto3
from botocore.exceptions import ClientError

from curator.exceptions import ActionError

# Import guards for optional dependencies
HAS_GCP = False
try:
    from google.cloud import storage
    from google.auth import exceptions as gauth_exceptions

    HAS_GCP = True
except ImportError:
    pass

HAS_AZURE = False
try:
    from azure.storage.blob import BlobServiceClient
    from azure.identity import DefaultAzureCredential
    from azure.core import exceptions as azure_exceptions

    HAS_AZURE = True
except ImportError:
    pass


class S3Client(metaclass=abc.ABCMeta):
    """
    Superclass for S3 Clients.

    This class should *only* perform actions that are common to all S3 clients. It
    should not handle record-keeping or anything unrelated to S3 actions. The calling
    methods should handle that.
    """

    @abc.abstractmethod
    def create_bucket(self, bucket_name: str) -> None:
        """
        Create a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to create.

        Returns:
            None
        """
        return

    @abc.abstractmethod
    def test_connection(self) -> bool:
        """
        Test S3 connection and validate credentials.

        :return: True if credentials are valid and S3 is accessible
        :rtype: bool
        """
        return

    @abc.abstractmethod
    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Test whether or not the named bucket exists

        :param bucket_name: Bucket name to check
        :type bucket_name: str
        :return: Existence state of named bucket
        :rtype: bool
        """
        return

    @abc.abstractmethod
    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: list[dict],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Return a bucket from deepfreeze.

        Args:
            bucket_name (str): The name of the bucket to return.
            base_path (str): The base path to the bucket to return.
            object_keys (list[dict]): A list of object metadata dictionaries (each containing 'Key', 'StorageClass', etc.).
            restore_days (int): The number of days to keep the object restored.
            retrieval_tier (str): The retrieval tier to use.

        Returns:
            None
        """
        return

    @abc.abstractmethod
    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "GLACIER"
    ) -> None:
        """
        Return a bucket to deepfreeze.

        Args:
            bucket_name (str): The name of the bucket to return.
            path (str): The path to the bucket to return.
            storage_class (str): The storage class to send the data to.

        """
        return

    @abc.abstractmethod
    def list_objects(self, bucket_name: str, prefix: str) -> list[dict]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[dict]: A list of object metadata dictionaries (each containing 'Key', 'StorageClass', etc.).
        """
        return

    @abc.abstractmethod
    def delete_bucket(self, bucket_name: str, force: bool = False) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.
            force (bool): If True, empty the bucket before deleting it.

        Returns:
            None
        """
        return

    @abc.abstractmethod
    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Put an object in a bucket at the given path.

        Args:
            bucket_name (str): The name of the bucket to put the object in.
            key (str): The key of the object to put.
            body (str): The body of the object to put.

        Returns:
            None
        """
        return

    @abc.abstractmethod
    def list_buckets(self, prefix: str = None) -> list[str]:
        """
        List all buckets.

        Returns:
            list[str]: A list of bucket names.
        """
        return

    @abc.abstractmethod
    def head_object(self, bucket_name: str, key: str) -> dict:
        """
        Retrieve metadata for an object without downloading it.

        Args:
            bucket_name (str): The name of the bucket.
            key (str): The object key.

        Returns:
            dict: Object metadata including Restore status if applicable.
        """
        return

    @abc.abstractmethod
    def copy_object(
        self,
        Bucket: str,
        Key: str,
        CopySource: dict[str, str],
        StorageClass: str,
    ) -> None:
        """
        Copy an object from one bucket to another.

        Args:
            source_bucket (str): The name of the source bucket.
            source_key (str): The key of the object to copy.
            dest_bucket (str): The name of the destination bucket.
            dest_key (str): The key for the copied object.

        Returns:
            None
        """
        return


class AwsS3Client(S3Client):
    """
    An S3 client object for use with AWS.
    """

    def __init__(self) -> None:
        self.loggit = logging.getLogger("AWS S3 Client")
        try:
            self.client = boto3.client("s3")
            # HIGH PRIORITY FIX: Validate credentials by attempting a simple operation
            self.loggit.debug("Validating AWS credentials")
            self.client.list_buckets()
            self.loggit.info("AWS S3 Client initialized successfully")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            self.loggit.error(
                "Failed to initialize AWS S3 Client: %s - %s", error_code, e
            )
            if error_code in ["InvalidAccessKeyId", "SignatureDoesNotMatch"]:
                raise ActionError(
                    "AWS credentials are invalid or not configured. "
                    "Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
                )
            elif error_code == "AccessDenied":
                raise ActionError(
                    "AWS credentials do not have sufficient permissions. "
                    "Minimum required: s3:ListAllMyBuckets"
                )
            raise ActionError(f"Failed to initialize AWS S3 Client: {e}")
        except Exception as e:
            self.loggit.error(
                "Failed to initialize AWS S3 Client: %s", e, exc_info=True
            )
            raise ActionError(f"Failed to initialize AWS S3 Client: {e}")

    def test_connection(self) -> bool:
        """
        Test S3 connection and validate credentials.

        :return: True if credentials are valid and S3 is accessible
        :rtype: bool
        """
        try:
            self.loggit.debug("Testing S3 connection")
            self.client.list_buckets()
            return True
        except ClientError as e:
            self.loggit.error("S3 connection test failed: %s", e)
            return False

    def create_bucket(self, bucket_name: str) -> None:
        self.loggit.info(f"Creating bucket: {bucket_name}")
        if self.bucket_exists(bucket_name):
            self.loggit.info(f"Bucket {bucket_name} already exists")
            raise ActionError(f"Bucket {bucket_name} already exists")
        try:
            # HIGH PRIORITY FIX: Add region handling for bucket creation
            # Get the region from the client configuration
            region = self.client.meta.region_name
            self.loggit.debug(f"Creating bucket in region: {region}")

            # AWS requires LocationConstraint for all regions except us-east-1
            if region and region != 'us-east-1':
                self.client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region},
                )
                self.loggit.info(
                    f"Successfully created bucket {bucket_name} in region {region}"
                )
            else:
                self.client.create_bucket(Bucket=bucket_name)
                self.loggit.info(
                    f"Successfully created bucket {bucket_name} in us-east-1"
                )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            self.loggit.error(
                f"Error creating bucket {bucket_name}: {error_code} - {e}"
            )
            raise ActionError(f"Error creating bucket {bucket_name}: {e}")

    def bucket_exists(self, bucket_name: str) -> bool:
        self.loggit.debug(f"Checking if bucket {bucket_name} exists")
        try:
            self.client.head_bucket(Bucket=bucket_name)
            self.loggit.debug(f"Bucket {bucket_name} exists")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.loggit.debug(f"Bucket {bucket_name} does not exist")
                return False
            else:
                self.loggit.error(
                    "Error checking bucket existence for %s: %s", bucket_name, e
                )
                raise ActionError(e)

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: list[dict],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Restores objects from Glacier storage class back to an instant access tier.

        Args:
            bucket_name (str): The name of the bucket
            base_path (str): The base path (prefix) of the objects to thaw
            object_keys (list[dict]): A list of object metadata dictionaries (each containing 'Key', 'StorageClass', etc.)
            restore_days (int): The number of days to keep the object restored
            retrieval_tier (str): The retrieval tier to use

        Returns:
            None
        """
        # ENHANCED LOGGING: Add parameter details and progress tracking
        self.loggit.info(
            "Starting thaw operation - bucket: %s, base_path: %s, objects: %d, restore_days: %d, tier: %s",
            bucket_name,
            base_path,
            len(object_keys),
            restore_days,
            retrieval_tier,
        )

        restored_count = 0
        skipped_count = 0
        error_count = 0

        for idx, obj in enumerate(object_keys, 1):
            # Extract key from object metadata dict
            key = obj.get("Key") if isinstance(obj, dict) else obj

            if not key.startswith(base_path):
                skipped_count += 1
                continue  # Skip objects outside the base path

            # Get storage class from object metadata (if available) or fetch it
            if isinstance(obj, dict) and "StorageClass" in obj:
                storage_class = obj.get("StorageClass", "")
            else:
                # ? Do we need to keep track of what tier this came from instead of just assuming Glacier?
                try:
                    response = self.client.head_object(Bucket=bucket_name, Key=key)
                    storage_class = response.get("StorageClass", "")
                except Exception as e:
                    error_count += 1
                    self.loggit.error(
                        "Error getting metadata for object %d/%d (%s): %s (type: %s)",
                        idx,
                        len(object_keys),
                        key,
                        str(e),
                        type(e).__name__,
                    )
                    continue

            try:
                if storage_class in ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]:
                    self.loggit.debug(
                        "Restoring object %d/%d: %s from %s",
                        idx,
                        len(object_keys),
                        key,
                        storage_class,
                    )
                    self.client.restore_object(
                        Bucket=bucket_name,
                        Key=key,
                        RestoreRequest={
                            "Days": restore_days,
                            "GlacierJobParameters": {"Tier": retrieval_tier},
                        },
                    )
                    restored_count += 1
                else:
                    self.loggit.debug(
                        "Skipping object %d/%d: %s (storage class: %s, not in Glacier)",
                        idx,
                        len(object_keys),
                        key,
                        storage_class,
                    )
                    skipped_count += 1

            except Exception as e:
                error_count += 1
                self.loggit.error(
                    "Error restoring object %d/%d (%s): %s (type: %s)",
                    idx,
                    len(object_keys),
                    key,
                    str(e),
                    type(e).__name__,
                )

        # Log summary
        self.loggit.info(
            "Thaw operation completed - restored: %d, skipped: %d, errors: %d (total: %d)",
            restored_count,
            skipped_count,
            error_count,
            len(object_keys),
        )

    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "GLACIER"
    ) -> None:
        """
        Moves objects back to a Glacier-tier storage class.

        Args:
            bucket_name (str): The name of the bucket
            path (str): The path to the objects to refreeze
            storage_class (str): The storage class to move the objects to

        Returns:
            None
        """
        # ENHANCED LOGGING: Add parameter details and progress tracking
        self.loggit.info(
            "Starting refreeze operation - bucket: %s, path: %s, target_storage_class: %s",
            bucket_name,
            path,
            storage_class,
        )

        refrozen_count = 0
        error_count = 0

        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=path)

        for page_num, page in enumerate(pages, 1):
            if "Contents" in page:
                page_objects = len(page["Contents"])
                self.loggit.debug(
                    "Processing page %d with %d objects", page_num, page_objects
                )

                for obj_num, obj in enumerate(page["Contents"], 1):
                    key = obj["Key"]
                    current_storage = obj.get("StorageClass", "STANDARD")

                    try:
                        # Copy the object with a new storage class
                        self.loggit.debug(
                            "Refreezing object %d/%d in page %d: %s (from %s to %s)",
                            obj_num,
                            page_objects,
                            page_num,
                            key,
                            current_storage,
                            storage_class,
                        )
                        self.client.copy_object(
                            Bucket=bucket_name,
                            CopySource={"Bucket": bucket_name, "Key": key},
                            Key=key,
                            StorageClass=storage_class,
                        )
                        refrozen_count += 1

                    except Exception as e:
                        error_count += 1
                        self.loggit.error(
                            "Error refreezing object %s: %s (type: %s)",
                            key,
                            str(e),
                            type(e).__name__,
                            exc_info=True,
                        )

        # Log summary
        self.loggit.info(
            "Refreeze operation completed - refrozen: %d, errors: %d",
            refrozen_count,
            error_count,
        )

    def list_objects(self, bucket_name: str, prefix: str) -> list[dict]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[dict]: A list of object metadata dictionaries (each containing 'Key', 'StorageClass', etc.).
        """
        self.loggit.info(
            f"Listing objects in bucket: {bucket_name} with prefix: {prefix}"
        )
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        objects = []

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects.append(obj)

        return objects

    def delete_bucket(self, bucket_name: str, force: bool = False) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.
            force (bool): If True, empty the bucket before deleting it.

        Returns:
            None
        """
        self.loggit.info(f"Deleting bucket: {bucket_name}")
        try:
            # If force=True, empty the bucket first
            if force:
                self.loggit.info(f"Emptying bucket {bucket_name} before deletion")
                try:
                    # List and delete all objects
                    paginator = self.client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=bucket_name)

                    for page in pages:
                        if 'Contents' in page:
                            objects = [{'Key': obj['Key']} for obj in page['Contents']]
                            if objects:
                                self.client.delete_objects(
                                    Bucket=bucket_name, Delete={'Objects': objects}
                                )
                                self.loggit.debug(
                                    f"Deleted {len(objects)} objects from {bucket_name}"
                                )
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchBucket':
                        self.loggit.warning(f"Error emptying bucket {bucket_name}: {e}")

            self.client.delete_bucket(Bucket=bucket_name)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)

    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Put an object in a bucket.

        Args:
            bucket_name (str): The name of the bucket to put the object in.
            key (str): The key of the object to put.
            body (str): The body of the object to put.

        Returns:
            None
        """
        self.loggit.info(f"Putting object: {key} in bucket: {bucket_name}")
        try:
            self.client.put_object(Bucket=bucket_name, Key=key, Body=body)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)

    def list_buckets(self, prefix: str = None) -> list[str]:
        """
        List all buckets.

        Returns:
            list[str]: A list of bucket names.
        """
        self.loggit.info("Listing buckets")
        try:
            response = self.client.list_buckets()
            buckets = response.get("Buckets", [])
            bucket_names = [bucket["Name"] for bucket in buckets]
            if prefix:
                bucket_names = [
                    name for name in bucket_names if name.startswith(prefix)
                ]
            return bucket_names
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)

    def head_object(self, bucket_name: str, key: str) -> dict:
        """
        Retrieve metadata for an object without downloading it.

        Args:
            bucket_name (str): The name of the bucket.
            key (str): The object key.

        Returns:
            dict: Object metadata including Restore status if applicable.
        """
        self.loggit.debug(f"Getting metadata for s3://{bucket_name}/{key}")
        try:
            response = self.client.head_object(Bucket=bucket_name, Key=key)
            return response
        except ClientError as e:
            self.loggit.error(f"Error getting metadata for {key}: {e}")
            raise ActionError(f"Error getting metadata for {key}: {e}")

    def copy_object(
        self,
        Bucket: str,
        Key: str,
        CopySource: dict[str, str],
        StorageClass: str = "GLACIER",
    ) -> None:
        """
        Copy an object from one bucket to another.

        Args:
            Bucket (str): The name of the destination bucket.
            Key (str): The key for the copied object.
            CopySource (dict[str, str]): The source bucket and key.
            StorageClass (str): The storage class to use.

        Returns:
            None
        """
        self.loggit.info(f"Copying object {Key} to bucket {Bucket}")
        try:
            self.client.copy_object(
                Bucket=Bucket,
                CopySource=CopySource,
                Key=Key,
                StorageClass=StorageClass,
            )
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(e)


class GcpStorageClient(S3Client):
    """
    A Cloud Storage client object for use with Google Cloud Platform (GCP).

    This client implements the S3Client interface for GCP Cloud Storage,
    mapping bucket/object operations to GCP-specific methods.
    """

    def __init__(self, location: str = "us-west1") -> None:
        self.loggit = logging.getLogger("GCP Storage Client")
        self.location = location
        try:
            # Initialize GCP Storage client with Application Default Credentials
            self.client = storage.Client()
            # Validate credentials by attempting a simple operation
            self.loggit.debug("Validating GCP credentials")
            # list_buckets() returns an iterator, consume it to trigger auth
            list(self.client.list_buckets(max_results=1))
            self.loggit.info("GCP Storage Client initialized successfully")
        except gauth_exceptions.DefaultCredentialsError as e:
            self.loggit.error("Failed to initialize GCP Storage Client: %s", e)
            raise ActionError(
                "GCP credentials are invalid or not configured. "
                "Set GOOGLE_APPLICATION_CREDENTIALS environment variable "
                "or run 'gcloud auth application-default login'"
            )
        except Exception as e:
            self.loggit.error(
                "Failed to initialize GCP Storage Client: %s", e, exc_info=True
            )
            raise ActionError(f"Failed to initialize GCP Storage Client: {e}")

    def test_connection(self) -> bool:
        """
        Test GCP Storage connection and validate credentials.

        :return: True if credentials are valid and GCP is accessible
        :rtype: bool
        """
        try:
            self.loggit.debug("Testing GCP Storage connection")
            list(self.client.list_buckets(max_results=1))
            return True
        except Exception as e:
            self.loggit.error("GCP Storage connection test failed: %s", e)
            return False

    def create_bucket(self, bucket_name: str) -> None:
        """
        Create a GCP Cloud Storage bucket.

        Args:
            bucket_name (str): The name of the bucket to create.

        Returns:
            None
        """
        self.loggit.info(f"Creating bucket: {bucket_name}")
        if self.bucket_exists(bucket_name):
            self.loggit.info(f"Bucket {bucket_name} already exists")
            raise ActionError(f"Bucket {bucket_name} already exists")
        try:
            bucket = self.client.bucket(bucket_name)
            bucket.location = self.location
            self.client.create_bucket(bucket, location=self.location)
            self.loggit.info(
                f"Successfully created bucket {bucket_name} in location {self.location}"
            )
        except Exception as e:
            self.loggit.error(f"Error creating bucket {bucket_name}: {e}")
            raise ActionError(f"Error creating bucket {bucket_name}: {e}")

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Test whether or not the named bucket exists.

        Args:
            bucket_name (str): Bucket name to check

        Returns:
            bool: Existence state of named bucket
        """
        self.loggit.debug(f"Checking if bucket {bucket_name} exists")
        try:
            bucket = self.client.bucket(bucket_name)
            exists = bucket.exists()
            self.loggit.debug(
                f"Bucket {bucket_name} {'exists' if exists else 'does not exist'}"
            )
            return exists
        except Exception as e:
            self.loggit.error(
                "Error checking bucket existence for %s: %s", bucket_name, e
            )
            raise ActionError(e)

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: list[dict],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Restore objects from Archive/Coldline storage to Standard.

        Note: GCP does not have the concept of restore days or retrieval tiers
        like AWS. Objects are moved directly to STANDARD storage class.

        Args:
            bucket_name (str): The name of the bucket
            base_path (str): The base path (prefix) of the objects to thaw
            object_keys (list[dict]): List of object metadata dicts
            restore_days (int): Ignored for GCP (no temporary restore)
            retrieval_tier (str): Ignored for GCP (no tiered retrieval)

        Returns:
            None
        """
        self.loggit.info(
            "Starting thaw operation - bucket: %s, base_path: %s, objects: %d",
            bucket_name,
            base_path,
            len(object_keys),
        )

        if restore_days != 7:
            self.loggit.warning(
                "GCP does not support temporary restore - "
                "restore_days parameter ignored"
            )
        if retrieval_tier != "Standard":
            self.loggit.warning(
                "GCP does not support retrieval tiers - "
                "retrieval_tier parameter ignored"
            )

        restored_count = 0
        skipped_count = 0
        error_count = 0

        bucket = self.client.bucket(bucket_name)

        for idx, obj in enumerate(object_keys, 1):
            # Extract key from object metadata dict
            key = obj.get("Key") if isinstance(obj, dict) else obj

            if not key.startswith(base_path):
                skipped_count += 1
                continue  # Skip objects outside the base path

            # Get storage class from object metadata (if available) or fetch it
            if isinstance(obj, dict) and "StorageClass" in obj:
                storage_class = obj.get("StorageClass", "")
            else:
                try:
                    blob = bucket.blob(key)
                    blob.reload()
                    storage_class = blob.storage_class or ""
                except Exception as e:
                    error_count += 1
                    self.loggit.error(
                        "Error getting metadata for object %d/%d (%s): %s (type: %s)",
                        idx,
                        len(object_keys),
                        key,
                        str(e),
                        type(e).__name__,
                    )
                    continue

            try:
                if storage_class in ["ARCHIVE", "COLDLINE"]:
                    self.loggit.debug(
                        "Restoring object %d/%d: %s from %s to STANDARD",
                        idx,
                        len(object_keys),
                        key,
                        storage_class,
                    )
                    blob = bucket.blob(key)
                    # Use rewrite to change storage class
                    blob.rewrite(blob, token=None)
                    blob.update_storage_class("STANDARD")
                    restored_count += 1
                else:
                    self.loggit.debug(
                        "Skipping object %d/%d: %s (storage class: %s, not Archive/Coldline)",
                        idx,
                        len(object_keys),
                        key,
                        storage_class,
                    )
                    skipped_count += 1

            except Exception as e:
                error_count += 1
                self.loggit.error(
                    "Error restoring object %d/%d (%s): %s (type: %s)",
                    idx,
                    len(object_keys),
                    key,
                    str(e),
                    type(e).__name__,
                )

        # Log summary
        self.loggit.info(
            "Thaw operation completed - restored: %d, skipped: %d, errors: %d (total: %d)",
            restored_count,
            skipped_count,
            error_count,
            len(object_keys),
        )

    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "ARCHIVE"
    ) -> None:
        """
        Move objects to Archive or Coldline storage class.

        Args:
            bucket_name (str): The name of the bucket
            path (str): The path prefix to the objects to refreeze
            storage_class (str): Target storage class (ARCHIVE or COLDLINE)

        Returns:
            None
        """
        self.loggit.info(
            "Starting refreeze operation - bucket: %s, path: %s, target_storage_class: %s",
            bucket_name,
            path,
            storage_class,
        )

        refrozen_count = 0
        error_count = 0

        bucket = self.client.bucket(bucket_name)
        # List blobs with prefix
        blobs = bucket.list_blobs(prefix=path)

        for blob in blobs:
            current_storage = blob.storage_class or "STANDARD"
            try:
                self.loggit.debug(
                    "Refreezing object: %s (from %s to %s)",
                    blob.name,
                    current_storage,
                    storage_class,
                )
                blob.update_storage_class(storage_class)
                refrozen_count += 1
            except Exception as e:
                error_count += 1
                self.loggit.error(
                    "Error refreezing object %s: %s (type: %s)",
                    blob.name,
                    str(e),
                    type(e).__name__,
                    exc_info=True,
                )

        # Log summary
        self.loggit.info(
            "Refreeze operation completed - refrozen: %d, errors: %d",
            refrozen_count,
            error_count,
        )

    def list_objects(self, bucket_name: str, prefix: str) -> list[dict]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[dict]: A list of object metadata dicts (GCP format converted to S3-like format).
        """
        self.loggit.info(
            f"Listing objects in bucket: {bucket_name} with prefix: {prefix}"
        )
        objects = []
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        for blob in blobs:
            # Convert GCP blob metadata to S3-like format
            objects.append(
                {
                    "Key": blob.name,
                    "Size": blob.size,
                    "StorageClass": blob.storage_class,
                    "LastModified": blob.updated,
                }
            )

        return objects

    def delete_bucket(self, bucket_name: str, force: bool = False) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.
            force (bool): If True, empty the bucket before deleting it.

        Returns:
            None
        """
        self.loggit.info(f"Deleting bucket: {bucket_name}")
        try:
            bucket = self.client.bucket(bucket_name)

            # If force=True, empty the bucket first
            if force:
                self.loggit.info(f"Emptying bucket {bucket_name} before deletion")
                try:
                    blobs = bucket.list_blobs()
                    for blob in blobs:
                        blob.delete()
                        self.loggit.debug(f"Deleted object {blob.name}")
                except Exception as e:
                    self.loggit.warning(f"Error emptying bucket {bucket_name}: {e}")

            bucket.delete()
            self.loggit.info(f"Successfully deleted bucket {bucket_name}")
        except Exception as e:
            self.loggit.error(e)
            raise ActionError(e)

    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Put an object in a bucket.

        Args:
            bucket_name (str): The name of the bucket to put the object in.
            key (str): The key of the object to put.
            body (str): The body of the object to put.

        Returns:
            None
        """
        self.loggit.info(f"Putting object: {key} in bucket: {bucket_name}")
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(key)
            blob.upload_from_string(body)
        except Exception as e:
            self.loggit.error(e)
            raise ActionError(e)

    def list_buckets(self, prefix: str = None) -> list[str]:
        """
        List all buckets.

        Args:
            prefix (str): Optional prefix filter for bucket names

        Returns:
            list[str]: A list of bucket names.
        """
        self.loggit.info("Listing buckets")
        try:
            buckets = self.client.list_buckets()
            bucket_names = [bucket.name for bucket in buckets]
            if prefix:
                bucket_names = [
                    name for name in bucket_names if name.startswith(prefix)
                ]
            return bucket_names
        except Exception as e:
            self.loggit.error(e)
            raise ActionError(e)

    def head_object(self, bucket_name: str, key: str) -> dict:
        """
        Retrieve metadata for an object without downloading it.

        Args:
            bucket_name (str): The name of the bucket.
            key (str): The object key.

        Returns:
            dict: Object metadata (GCP format converted to S3-like format).
        """
        self.loggit.debug(f"Getting metadata for gs://{bucket_name}/{key}")
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(key)
            blob.reload()

            # Convert GCP metadata to S3-like format
            return {
                "ContentLength": blob.size,
                "StorageClass": blob.storage_class,
                "LastModified": blob.updated,
                "ETag": blob.etag,
                "ContentType": blob.content_type,
            }
        except Exception as e:
            self.loggit.error(f"Error getting metadata for {key}: {e}")
            raise ActionError(f"Error getting metadata for {key}: {e}")

    def copy_object(
        self,
        Bucket: str,
        Key: str,
        CopySource: dict[str, str],
        StorageClass: str = "ARCHIVE",
    ) -> None:
        """
        Copy an object with a new storage class.

        Args:
            Bucket (str): The name of the destination bucket.
            Key (str): The key for the copied object.
            CopySource (dict[str, str]): Source bucket and key.
            StorageClass (str): The storage class to use.

        Returns:
            None
        """
        self.loggit.info(f"Copying object {Key} to bucket {Bucket}")
        try:
            source_bucket_name = CopySource["Bucket"]
            source_key = CopySource["Key"]

            source_bucket = self.client.bucket(source_bucket_name)
            source_blob = source_bucket.blob(source_key)

            dest_bucket = self.client.bucket(Bucket)
            dest_blob = dest_bucket.blob(Key)

            # Copy with rewrite to change storage class
            token = None
            while True:
                token, bytes_rewritten, total_bytes = dest_blob.rewrite(
                    source_blob, token=token
                )
                if token is None:
                    break

            # Update storage class
            dest_blob.update_storage_class(StorageClass)
        except Exception as e:
            self.loggit.error(e)
            raise ActionError(e)


def s3_client_factory(provider: str) -> S3Client:
    """
    s3_client_factory method, returns an S3Client object implemented specific to
    the value of the provider argument.

    Args:
        provider (str): The provider to use for the S3Client object. Should
                        reference an implemented provider (aws, gcp, azure, etc)

    Raises:
        ActionError: raised if the provider requires optional dependencies that are not installed
        ValueError: raised if the provider string is invalid.

    Returns:
        S3Client: An S3Client object specific to the provider argument.
    """
    if provider == "aws":
        return AwsS3Client()
    elif provider == "gcp":
        if not HAS_GCP:
            raise ActionError(
                "GCP support requires google-cloud-storage library. "
                "Install with: pip install elasticsearch-curator[gcp]"
            )
        return GcpStorageClient()
    elif provider == "azure":
        # Azure support to be implemented
        if not HAS_AZURE:
            raise ActionError(
                "Azure support requires azure-storage-blob and azure-identity libraries. "
                "Install with: pip install elasticsearch-curator[azure]"
            )
        raise NotImplementedError("Azure S3Client is not implemented yet")
    else:
        raise ValueError(f"Unsupported provider: {provider}")
