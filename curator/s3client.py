"""
s3client.py

import boto3

Encapsulate the S3 client here so it can be used by all Curator classes, not just
deepfreeze.
"""

import abc
import logging
import os
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from curator.exceptions import ActionError

# from botocore.exceptions import ClientError


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
        object_keys: list[str],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Return a bucket from deepfreeze.

        Args:
            bucket_name (str): The name of the bucket to return.
            path (str): The path to the bucket to return.
            object_keys (list[str]): A list of object keys to return.
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
    def list_objects(self, bucket_name: str, prefix: str) -> list[str]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[str]: A list of object keys.
        """
        return

    @abc.abstractmethod
    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.

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
    def copy_object(
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


class AzureS3Client(S3Client):
    """
    An S3 client object for use with Azure Blob Storage via S3-compatible API.
    """

    def __init__(self) -> None:
        """
        Initialize the Azure S3 client using environment variables for credentials.
        """
        self.logger = logging.getLogger("Azure S3 Client")
        try:
            # Azure Blob Storage S3-compatible endpoint and credentials
            endpoint_url = os.getenv("AZURE_S3_ENDPOINT_URL")
            access_key = os.getenv("AZURE_ACCESS_KEY")
            secret_key = os.getenv("AZURE_SECRET_KEY")

            if not all([endpoint_url, access_key, secret_key]):
                raise ValueError(
                    "Missing required environment variables for Azure S3 client"
                )

            # Initialize boto3 client for Azure Blob Storage
            self.client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            self.logger.info("Azure S3 client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure S3 client: {str(e)}")
            raise

    def create_bucket(self, bucket_name: str) -> None:
        """
        Create a bucket (container) in Azure Blob Storage.

        Args:
            bucket_name (str): Name of the bucket to create
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket {bucket_name} created successfully")
        except ClientError as e:
            self.logger.error(f"Failed to create bucket {bucket_name}: {str(e)}")
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists in Azure Blob Storage.

        Args:
            bucket_name (str): Name of the bucket to check

        Returns:
            bool: True if bucket exists, False otherwise
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            self.logger.debug(f"Bucket {bucket_name} exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.debug(f"Bucket {bucket_name} does not exist")
                return False
            self.logger.error(f"Error checking bucket {bucket_name}: {str(e)}")
            raise

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: List[str],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Restore objects from archive (not directly supported in Azure).

        Args:
            bucket_name (str): Name of the bucket
            base_path (str): Base path for objects
            object_keys (List[str]): List of object keys to restore
            restore_days (int): Number of days to keep restored objects
            retrieval_tier (str): Retrieval tier (Standard, Expedited, etc.)

        Note: Azure Blob Storage doesn't have a direct equivalent to S3 Glacier.
              This method logs a warning and skips restoration.
        """
        self.logger.warning(
            "Azure Blob Storage does not support Glacier-like storage classes or thaw operations. "
            "Objects are assumed to be in hot tier by default."
        )
        # If Azure introduces archive tiers in the future, this could be updated
        for key in object_keys:
            full_key = f"{base_path}/{key}" if base_path else key
            self.logger.info(f"Skipping thaw for {full_key} (not applicable in Azure)")

    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "GLACIER"
    ) -> None:
        """
        Move an object to a colder storage class (not directly supported in Azure).

        Args:
            bucket_name (str): Name of the bucket
            path (str): Path to the object
            storage_class (str): Target storage class (e.g., GLACIER)

        Note: Azure Blob Storage supports hot, cool, and archive tiers, but not via S3 API.
              This method logs a warning and skips the operation.
        """
        self.logger.warning(
            "Azure Blob Storage does not support Glacier-like storage classes via S3 API. "
            "Use Azure-native APIs to change to cool or archive tiers."
        )
        self.logger.info(
            f"Skipping refreeze for {path} to {storage_class} (not applicable in Azure)"
        )

    def list_objects(self, bucket_name: str, prefix: str) -> List[str]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): Name of the bucket
            prefix (str): Prefix to filter objects

        Returns:
            List[str]: List of object keys
        """
        try:
            response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            objects = [obj['Key'] for obj in response.get('Contents', [])]
            self.logger.debug(
                f"Listed {len(objects)} objects in {bucket_name} with prefix {prefix}"
            )
            return objects
        except ClientError as e:
            self.logger.error(f"Failed to list objects in {bucket_name}: {str(e)}")
            raise

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket in Azure Blob Storage.

        Args:
            bucket_name (str): Name of the bucket to delete
        """
        try:
            # Azure requires buckets to be empty before deletion
            objects = self.list_objects(bucket_name, "")
            if objects:
                self.logger.warning(
                    f"Bucket {bucket_name} is not empty; deleting objects first"
                )
                for obj in objects:
                    self.client.delete_object(Bucket=bucket_name, Key=obj)
            self.client.delete_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket {bucket_name} deleted successfully")
        except ClientError as e:
            self.logger.error(f"Failed to delete bucket {bucket_name}: {str(e)}")
            raise

    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Upload an object to a bucket.

        Args:
            bucket_name (str): Name of the bucket
            key (str): Object key
            body (str): Object content as a string
        """
        try:
            self.client.put_object(
                Bucket=bucket_name, Key=key, Body=body.encode('utf-8')
            )
            self.logger.info(f"Object {key} uploaded to {bucket_name}")
        except ClientError as e:
            self.logger.error(
                f"Failed to upload object {key} to {bucket_name}: {str(e)}"
            )
            raise

    def list_buckets(self, prefix: Optional[str] = None) -> List[str]:
        """
        List all buckets, optionally filtering by prefix.

        Args:
            prefix (Optional[str]): Prefix to filter bucket names

        Returns:
            List[str]: List of bucket names
        """
        try:
            response = self.client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            if prefix:
                buckets = [b for b in buckets if b.startswith(prefix)]
            self.logger.debug(
                f"Listed {len(buckets)} buckets with prefix {prefix or 'none'}"
            )
            return buckets
        except ClientError as e:
            self.logger.error(f"Failed to list buckets: {str(e)}")
            raise

    def copy_object(
        self,
        Bucket: str,
        Key: str,
        CopySource: Dict[str, str],
        StorageClass: str = "GLACIER",
    ) -> None:
        """
        Copy an object within or across buckets.

        Args:
            Bucket (str): Destination bucket
            Key (str): Destination object key
            CopySource (Dict[str, str]): Source bucket and key (e.g., {'Bucket': 'source', 'Key': 'key'})
            StorageClass (str): Storage class for the copied object

        Note: Azure Blob Storage doesn't support Glacier via S3 API; StorageClass is ignored.
        """
        try:
            self.client.copy_object(Bucket=Bucket, Key=Key, CopySource=CopySource)
            self.logger.info(f"Copied object from {CopySource} to {Bucket}/{Key}")
            if StorageClass == "GLACIER":
                self.logger.warning(
                    "GLACIER storage class not supported in Azure; using default tier"
                )
        except ClientError as e:
            self.logger.error(f"Failed to copy object to {Bucket}/{Key}: {str(e)}")
            raise


class GCPS3Client(S3Client):
    """
    An S3 client object for use with Google Cloud Storage via S3-compatible API.
    """

    def __init__(self) -> None:
        """
        Initialize the GCP S3 client using environment variables for credentials.
        """
        self.logger = logging.getLogger("GCP S3 Client")
        try:
            # GCP S3-compatible endpoint and credentials
            endpoint_url = os.getenv(
                "GCP_S3_ENDPOINT_URL", "https://storage.googleapis.com"
            )
            access_key = os.getenv("GCP_ACCESS_KEY")
            secret_key = os.getenv("GCP_SECRET_KEY")

            if not all([access_key, secret_key]):
                raise ValueError(
                    "Missing required environment variables for GCP S3 client"
                )

            # Initialize boto3 client for GCS
            self.client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            self.logger.info("GCP S3 client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP S3 client: {str(e)}")
            raise

    def create_bucket(self, bucket_name: str) -> None:
        """
        Create a bucket in Google Cloud Storage.

        Args:
            bucket_name (str): Name of the bucket to create
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket {bucket_name} created successfully")
        except ClientError as e:
            self.logger.error(f"Failed to create bucket {bucket_name}: {str(e)}")
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists in Google Cloud Storage.

        Args:
            bucket_name (str): Name of the bucket to check

        Returns:
            bool: True if bucket exists, False otherwise
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            self.logger.debug(f"Bucket {bucket_name} exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.debug(f"Bucket {bucket_name} does not exist")
                return False
            self.logger.error(f"Error checking bucket {bucket_name}: {str(e)}")
            raise

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: List[str],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Restore objects from ARCHIVE storage class in GCS.

        Args:
            bucket_name (str): Name of the bucket
            base_path (str): Base path for objects
            object_keys (List[str]): List of object keys to restore
            restore_days (int): Number of days to keep restored objects
            retrieval_tier (str): Retrieval tier (Standard, Bulk)
        """
        try:
            for key in object_keys:
                full_key = f"{base_path}/{key}" if base_path else key
                # Check if object is in ARCHIVE and needs restoration
                response = self.client.head_object(Bucket=bucket_name, Key=full_key)
                storage_class = response.get('StorageClass', 'STANDARD')
                if storage_class == 'ARCHIVE':
                    self.client.restore_object(
                        Bucket=bucket_name,
                        Key=full_key,
                        RestoreRequest={
                            'Days': restore_days,
                            'GlacierJobParameters': {
                                'Tier': (
                                    retrieval_tier
                                    if retrieval_tier in ['Standard', 'Bulk']
                                    else 'Standard'
                                )
                            },
                        },
                    )
                    self.logger.info(
                        f"Initiated restore for {full_key} for {restore_days} days"
                    )
                else:
                    self.logger.info(
                        f"Object {full_key} is not in ARCHIVE; no restore needed"
                    )
        except ClientError as e:
            self.logger.error(f"Failed to restore objects in {bucket_name}: {str(e)}")
            raise

    def refreeze(
        self, bucket_name: str, path: str, storage_class: str = "ARCHIVE"
    ) -> None:
        """
        Move an object to ARCHIVE storage class in GCS.

        Args:
            bucket_name (str): Name of the bucket
            path (str): Path to the object
            storage_class (str): Target storage class (e.g., ARCHIVE)
        """
        try:
            # Copy object to itself with new storage class
            copy_source = {'Bucket': bucket_name, 'Key': path}
            self.client.copy_object(
                Bucket=bucket_name,
                Key=path,
                CopySource=copy_source,
                StorageClass=(
                    storage_class
                    if storage_class in ['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE']
                    else 'ARCHIVE'
                ),
                MetadataDirective='COPY',
            )
            self.logger.info(f"Object {path} moved to {storage_class} storage class")
        except ClientError as e:
            self.logger.error(f"Failed to refreeze {path} to {storage_class}: {str(e)}")
            raise

    def list_objects(self, bucket_name: str, prefix: str) -> List[str]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): Name of the bucket
            prefix (str): Prefix to filter objects

        Returns:
            List[str]: List of object keys
        """
        try:
            response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            objects = [obj['Key'] for obj in response.get('Contents', [])]
            self.logger.debug(
                f"Listed {len(objects)} objects in {bucket_name} with prefix {prefix}"
            )
            return objects
        except ClientError as e:
            self.logger.error(f"Failed to list objects in {bucket_name}: {str(e)}")
            raise

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket in Google Cloud Storage.

        Args:
            bucket_name (str): Name of the bucket to delete
        """
        try:
            # GCS requires buckets to be empty before deletion
            objects = self.list_objects(bucket_name, "")
            if objects:
                self.logger.warning(
                    f"Bucket {bucket_name} is not empty; deleting objects first"
                )
                for obj in objects:
                    self.client.delete_object(Bucket=bucket_name, Key=obj)
            self.client.delete_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket {bucket_name} deleted successfully")
        except ClientError as e:
            self.logger.error(f"Failed to delete bucket {bucket_name}: {str(e)}")
            raise

    def put_object(self, bucket_name: str, key: str, body: str = "") -> None:
        """
        Upload an object to a bucket.

        Args:
            bucket_name (str): Name of the bucket
            key (str): Object key
            body (str): Object content as a string
        """
        try:
            self.client.put_object(
                Bucket=bucket_name, Key=key, Body=body.encode('utf-8')
            )
            self.logger.info(f"Object {key} uploaded to {bucket_name}")
        except ClientError as e:
            self.logger.error(
                f"Failed to upload object {key} to {bucket_name}: {str(e)}"
            )
            raise

    def list_buckets(self, prefix: Optional[str] = None) -> List[str]:
        """
        List all buckets, optionally filtering by prefix.

        Args:
            prefix (Optional[str]): Prefix to filter bucket names

        Returns:
            List[str]: List of bucket names
        """
        try:
            response = self.client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            if prefix:
                buckets = [b for b in buckets if b.startswith(prefix)]
            self.logger.debug(
                f"Listed {len(buckets)} buckets with prefix {prefix or 'none'}"
            )
            return buckets
        except ClientError as e:
            self.logger.error(f"Failed to list buckets: {str(e)}")
            raise

    def copy_object(
        self,
        Bucket: str,
        Key: str,
        CopySource: Dict[str, str],
        StorageClass: str = "ARCHIVE",
    ) -> None:
        """
        Copy an object within or across buckets.

        Args:
            Bucket (str): Destination bucket
            Key (str): Destination object key
            CopySource (Dict[str, str]): Source bucket and key (e.g., {'Bucket': 'source', 'Key': 'key'})
            StorageClass (str): Storage class for the copied object (e.g., ARCHIVE)
        """
        try:
            self.client.copy_object(
                Bucket=Bucket,
                Key=Key,
                CopySource=CopySource,
                StorageClass=(
                    StorageClass
                    if StorageClass in ['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE']
                    else 'STANDARD'
                ),
            )
            self.logger.info(
                f"Copied object from {CopySource} to {Bucket}/{Key} with storage class {StorageClass}"
            )
        except ClientError as e:
            self.logger.error(f"Failed to copy object to {Bucket}/{Key}: {str(e)}")
            raise


class AwsS3Client(S3Client):
    """
    An S3 client object for use with AWS.
    """

    def __init__(self) -> None:
        self.client = boto3.client("s3")
        self.loggit = logging.getLogger("AWS S3 Client")

    def create_bucket(self, bucket_name: str) -> None:
        self.loggit.info(f"Creating bucket: {bucket_name}")
        if self.bucket_exists(bucket_name):
            self.loggit.info(f"Bucket {bucket_name} already exists")
            raise ActionError(f"Bucket {bucket_name} already exists")
        try:
            self.client.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            self.loggit.error(e)
            raise ActionError(f"Error creating bucket {bucket_name}: {e}")

    def bucket_exists(self, bucket_name: str) -> bool:
        self.loggit.info(f"Checking if bucket {bucket_name} exists")
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                self.loggit.error(e)
                raise ActionError(e)

    def thaw(
        self,
        bucket_name: str,
        base_path: str,
        object_keys: list[str],
        restore_days: int = 7,
        retrieval_tier: str = "Standard",
    ) -> None:
        """
        Restores objects from Glacier storage class back to an instant access tier.

        Args:
            bucket_name (str): The name of the bucket
            base_path (str): The base path (prefix) of the objects to thaw
            object_keys (list[str]): A list of object keys to thaw
            restore_days (int): The number of days to keep the object restored
            retrieval_tier (str): The retrieval tier to use

        Returns:
            None
        """
        self.loggit.info(f"Thawing bucket: {bucket_name} at path: {base_path}")
        for key in object_keys:
            if not key.startswith(base_path):
                continue  # Skip objects outside the base path

            # ? Do we need to keep track of what tier this came from instead of just assuming Glacier?
            try:
                response = self.client.head_object(Bucket=bucket_name, Key=key)
                storage_class = response.get("StorageClass", "")

                if storage_class in ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]:
                    self.loggit.debug(f"Restoring: {key} from {storage_class})")
                    self.client.restore_object(
                        Bucket=bucket_name,
                        Key=key,
                        RestoreRequest={
                            "Days": restore_days,
                            "GlacierJobParameters": {"Tier": retrieval_tier},
                        },
                    )
                else:
                    self.loggit.debug(
                        f"Skipping: {key} (Storage Class: {storage_class})"
                    )

            except Exception as e:
                self.loggit.error(f"Error restoring {key}: {str(e)}")

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
        self.loggit.info(f"Refreezing objects in bucket: {bucket_name} at path: {path}")

        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=path)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]

                    try:
                        # Copy the object with a new storage class
                        self.client.copy_object(
                            Bucket=bucket_name,
                            CopySource={"Bucket": bucket_name, "Key": key},
                            Key=key,
                            StorageClass=storage_class,
                        )
                        self.loggit.info(f"Refrozen: {key} to {storage_class}")

                    except Exception as e:
                        self.loggit.error(f"Error refreezing {key}: {str(e)}")

    def list_objects(self, bucket_name: str, prefix: str) -> list[str]:
        """
        List objects in a bucket with a given prefix.

        Args:
            bucket_name (str): The name of the bucket to list objects from.
            prefix (str): The prefix to use when listing objects.

        Returns:
            list[str]: A list of object keys.
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

    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a bucket with the given name.

        Args:
            bucket_name (str): The name of the bucket to delete.

        Returns:
            None
        """
        self.loggit.info(f"Deleting bucket: {bucket_name}")
        try:
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


def s3_client_factory(provider: str) -> S3Client:
    """
    s3_client_factory method, returns an S3Client object implemented specific to
    the value of the provider argument.

    Args:
        provider (str): The provider to use for the S3Client object. Should
                        reference an implemented provider (aws, gcp, azure, etc)

    Raises:
        NotImplementedError: raised if the provider is not implemented
        ValueError: raised if the provider string is invalid.

    Returns:
        S3Client: An S3Client object specific to the provider argument.
    """
    if provider == "aws":
        return AwsS3Client()
    elif provider == "gcp":
        # Placeholder for GCP S3Client implementation
        raise NotImplementedError("GCP S3Client is not implemented yet")
    elif provider == "azure":
        # Placeholder for Azure S3Client implementation
        raise NotImplementedError("Azure S3Client is not implemented yet")
    else:
        raise ValueError(f"Unsupported provider: {provider}")
