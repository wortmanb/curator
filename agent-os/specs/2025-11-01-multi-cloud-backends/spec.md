# Specification: Multi-Cloud Backend Support for Deepfreeze

## Goal
Expand Elasticsearch Curator's deepfreeze functionality to support Google Cloud Platform (GCP) Cloud Storage and Microsoft Azure Blob Storage in addition to AWS S3/Glacier, enabling users to choose their preferred cloud provider for cold storage operations.

## User Stories
- As a DevOps engineer using GCP, I want to deepfreeze Elasticsearch snapshots to Google Cloud Storage Archive tier so that I can reduce storage costs while maintaining data retention requirements
- As an infrastructure administrator using Azure, I want to deepfreeze Elasticsearch snapshots to Azure Blob Storage Archive tier so that I can leverage our existing Azure infrastructure and billing
- As a multi-cloud platform team, I want to configure storage class mappings per provider in curator.yml so that I can control cost vs. access time tradeoffs for each cloud environment

## Specific Requirements

**GCP Cloud Storage Client Implementation**
- Create GcpStorageClient class extending S3Client abstract base class
- Implement all 11 abstract methods from S3Client interface matching AWS implementation patterns
- Use google-cloud-storage SDK with Application Default Credentials (ADC) for authentication
- Validate credentials on initialization by calling list_buckets() and catching authentication errors
- Support both Archive and Coldline storage classes for cold storage operations
- Handle GCP-specific pagination for list operations using bucket.list_blobs()
- Map create_bucket/bucket_exists/delete_bucket to GCP bucket operations
- Map put_object/head_object/copy_object to GCP blob operations

**Azure Blob Storage Client Implementation**
- Create AzureBlobClient class extending S3Client abstract base class
- Implement all 11 abstract methods from S3Client interface matching AWS implementation patterns
- Use azure-storage-blob SDK with DefaultAzureCredential for authentication
- Validate credentials on initialization by calling list_containers() and catching authentication errors
- Support Archive and Cool storage tiers with Standard/High rehydration priorities
- Handle Azure-specific pagination for list operations using continuation tokens
- Map bucket operations to Azure container operations (bucket -> container terminology)
- Map object operations to Azure blob operations with appropriate access tier handling

**Factory Pattern Extension**
- Update s3_client_factory() function to support "gcp" and "azure" provider strings
- Add import guards to check for optional dependencies before instantiation
- Raise ActionError with clear installation instructions when optional dependency is missing
- Format: "GCP support requires: pip install elasticsearch-curator[gcp]"
- Maintain backward compatibility with existing "aws" provider

**Configuration-Driven Storage Class Mapping**
- Add new deepfreeze section to curator.yml in user's home directory (~/.curator/curator.yml)
- Include provider selection field with options: aws, gcp, azure
- Add provider-specific subsections (aws, gcp, azure) with storage_class_map dictionaries
- Map logical names (cold, deep_cold, intelligent, standard) to provider-specific storage classes
- Include region/location configuration per provider (AWS: region, GCP: location, Azure: location)
- Add retrieval tier configuration for AWS (Standard/Expedited/Bulk) defaulting to Standard
- Add rehydration_priority for Azure (Standard/High) defaulting to Standard
- Update examples/curator.yml with documented examples for all three providers

**Credential Validation Pattern**
- Follow AWS pattern of validating credentials immediately on client initialization
- GCP: call storage_client.list_buckets() and catch google.auth.exceptions.DefaultCredentialsError
- Azure: call blob_service_client.list_containers() and catch azure.core.exceptions.ClientAuthenticationError
- Wrap provider-specific exceptions in ActionError with helpful error messages
- Include instructions for credential setup in error messages (GOOGLE_APPLICATION_CREDENTIALS, Azure CLI, etc.)

**Optional Dependencies Management**
- Add [project.optional-dependencies] section to pyproject.toml
- Create gcp extra with google-cloud-storage>=2.0.0
- Create azure extra with azure-storage-blob>=12.0.0 and azure-identity>=1.12.0
- Create all extra that includes both GCP and Azure dependencies for convenience
- Keep boto3 as required dependency for backward compatibility with existing AWS users

**Storage Class Filtering in Thaw Operations**
- GCP: Filter objects by storage_class matching ["ARCHIVE", "COLDLINE"] before restoring
- Azure: Filter blobs by access_tier matching ["Archive"] before rehydration
- AWS: Keep existing filter for ["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"]
- Use head_object/get_blob_properties to fetch storage class metadata when not available in list results

**Region and Location Handling**
- AWS: Use client.meta.region_name, handle us-east-1 exception (no LocationConstraint needed)
- GCP: Use bucket.location property, support region (e.g., us-west1) or multi-region (e.g., us)
- Azure: Specify account_url with region in storage account endpoint (e.g., https://account.westus2.blob.core.windows.net)
- Read region/location from deepfreeze configuration section per provider

**Error Handling and Logging**
- Use existing ActionError exception from curator.exceptions for all failures
- Follow AWS logging patterns with descriptive messages and progress tracking
- Log operation start with parameters, count successes/failures, log summary at end
- Include exc_info=True when logging unexpected errors
- Use provider-specific error code extraction where available

**Progress Tracking for Batch Operations**
- Implement thaw operation with object-by-object counting (restored/skipped/error counts)
- Implement refreeze operation with pagination and page-level progress logging
- Use rich library for formatted console output in interactive mode
- Support porcelain mode for machine-readable tab-separated output

## Visual Design
No visual assets provided.

## Existing Code to Leverage

**curator/s3client.py - AwsS3Client class**
- Follow exact method signature patterns for all 11 abstract methods
- Replicate credential validation pattern in __init__ (lines 198-222)
- Use same error handling approach with ActionError wrapping
- Follow logging style with debug/info/error levels and progress tracking
- Apply pagination pattern from list_objects and refreeze methods

**curator/s3client.py - S3Client abstract base class**
- All method signatures and docstrings are already defined
- Type hints specify expected parameters and return types
- Abstract methods enforce implementation contract across all providers

**curator/actions/deepfreeze/setup.py - Setup class**
- Reuse s3_client_factory() call pattern (line 100)
- Reference settings.provider for dynamic client selection
- Follow precondition checking pattern for validation
- Use rich Console and Panel for formatted error messages

**curator/actions/deepfreeze/helpers.py - Settings dataclass**
- Extend Settings to include deepfreeze configuration dictionary
- Add parsing logic to load provider-specific configuration from curator.yml
- Maintain existing attributes for backward compatibility

**curator/actions/deepfreeze/thaw.py and refreeze.py**
- Use s3.thaw() and s3.refreeze() method calls which work polymorphically
- No changes needed since they call abstract interface methods
- Provider-specific logic encapsulated in client implementations

## Out of Scope
- Cross-provider migration or data movement between AWS, GCP, and Azure
- Automatic provider selection based on cost or performance metrics
- Cost comparison and reporting across providers
- Enhanced setup validation for provider-specific quotas or billing checks
- Parallel thaw operations for faster retrieval
- Smart retrieval tier selection based on data size or urgency
- Automated retention policy management
- Monitoring and observability dashboards for multi-cloud operations
- Repository health checks beyond existing Elasticsearch validation
- Compression optimization or deduplication across providers
- Changes to deepfreeze action workflow (setup, thaw, refreeze commands unchanged)
- UI/CLI changes beyond what's needed for provider selection
- Breaking changes to existing AWS-only configurations (full backward compatibility required)
- Schema validation updates (to be handled as separate task)
- Integration test harness setup (follow existing test patterns)
