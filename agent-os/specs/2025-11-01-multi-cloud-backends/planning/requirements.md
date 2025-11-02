# Spec Requirements: Multi-Cloud Backend Support for Deepfreeze

## Initial Description

Implement multi-cloud backend support for Elasticsearch Curator's deepfreeze functionality. This constitutes the next development phase and includes three priorities:

1. **GCP Cloud Storage Backend Integration**: Add support for Google Cloud Platform's Cloud Storage as a backend option for deepfreeze operations.

2. **Azure Blob Storage Backend Integration**: Add support for Microsoft Azure's Blob Storage as a backend option for deepfreeze operations.

3. **Cloud Provider Selection Framework Enhancement**: The --provider option already exists in the setup action but may need tweaking to support the new backends.

### Current Context
- Currently, deepfreeze only supports AWS S3/Glacier via boto3
- The existing code has `curator/s3client.py` and `curator/actions/deepfreeze/`
- There's already a --provider option in the setup action that handles provider selection
- The goal is to expand from AWS-only to multi-cloud (AWS, GCP, Azure)

## Requirements Discussion

### First Round Questions

**Q1: How should we handle storage class configuration for each provider?**

**Context**: AWS uses GLACIER/DEEP_ARCHIVE, GCP uses ARCHIVE/COLDLINE, Azure uses Archive tier. These need to be mapped or configured.

**Answer**: Don't hardcode anything. Replace with a mapping in the curator.yml file. Location: User's homedir at `~/.curator/curator.yml`. Add a new **deepfreeze section** with these mappings. Update `examples/curator.yml` to show the new structure.

**Q2: Should credential validation happen on client initialization (like AWS currently does) or lazily on first operation?**

**Context**: The current AwsS3Client validates credentials immediately by calling `list_buckets()` in `__init__`. This catches auth issues early.

**Answer**: Follow the current pattern (validate on client initialization). User is not familiar with Azure or GCP, so **open to suggestions** about the best validation calls to make.

**Q3: For storage tier defaults, should we default to Standard retrieval tier for AWS and Azure, or offer Expedited/High/Bulk options?**

**Context**: AWS offers Standard, Expedited, and Bulk retrieval. Azure has Standard, High, and no Bulk equivalent. This impacts cost and speed.

**Answer**: The **retrieval_tier** selection for AWS and Azure should default to **Standard** (not Expedited, High, or Bulk). This clarifies it's about retrieval tier, not storage class.

**Q4: Should google-cloud-storage and azure-storage-blob be required dependencies or optional extras?**

**Context**: Users only need the provider they're using. Optional dependencies reduce installation size and complexity.

**Answer**: Make google-cloud-storage and azure-storage-blob **standalone extras** for now. Format: `pip install elasticsearch-curator[gcp]` or `pip install elasticsearch-curator[azure]`.

**Q5: Should we rename S3Client to something more generic (like CloudStorageClient) or keep the name since it's already in use?**

**Context**: The abstract base class is called S3Client but will support non-S3 providers. This might be confusing but avoids breaking changes.

**Answer**: Keep the **application-facing method names as-is** (create_bucket, bucket_exists, etc.). Within each method, call appropriate provider-specific methods for Azure, GCP, and AWS. Look for the **most pythonic way** to implement this.

**Q6: For GCP, should we support both Archive and Coldline storage classes, or just Archive?**

**Context**: GCP has multiple cold storage tiers: Archive (lowest cost, highest latency) and Coldline (medium cost/latency).

**Answer**: Support both Archive and Coldline storage classes for GCP to give users flexibility in cost vs. access time tradeoffs.

**Q7: Should Azure's rehydration priority (High/Standard) be configurable or always use Standard?**

**Context**: Azure offers High and Standard rehydration priorities when restoring from Archive tier, similar to AWS retrieval tiers.

**Answer**: Make it configurable and default to Standard (matching the AWS pattern).

**Q8: How should we handle region/location configuration for GCP and Azure?**

**Context**: AWS buckets are regional. GCP buckets have location types (region/multi-region). Azure has storage account regions.

**Answer**: Add region/location configuration to the deepfreeze section of curator.yml. Make it provider-specific so each provider can specify appropriate location settings.

**Q9: Should the factory pattern in s3_client_factory remain, or should we use a different initialization pattern?**

**Context**: Currently there's a factory function that returns the appropriate client based on provider string.

**Answer**: Keep the factory pattern as it's already working well. Just extend it to support "gcp" and "azure" providers.

**Q10: What should happen if a user tries to use a provider without the optional dependency installed?**

**Context**: If someone tries `--provider gcp` without `pip install elasticsearch-curator[gcp]`, we need to handle it gracefully.

**Answer**: Raise a clear error message during client initialization that tells them to install the appropriate extra: "GCP support requires: pip install elasticsearch-curator[gcp]"

**Q11: Are there existing features with similar patterns we should reference?**

**Answer**: Use the current S3Client code (AWS-specific) and try to use the same methods whenever possible. The existing AwsS3Client implementation in `curator/s3client.py` should serve as the reference pattern for GcpStorageClient and AzureBlobClient.

**Q12: Do you have any design mockups or visual assets?**

**Answer**: No visual assets provided.

### Existing Code to Reference

**Similar Features Identified:**
- Feature: AWS S3 Client Implementation - Path: `/Users/bret/git/curator/curator/s3client.py`
- Abstract base class pattern: The S3Client abstract base class defines the interface that all provider implementations must follow
- Method implementations to reference: AwsS3Client methods show the pattern for credential validation, error handling, logging, and progress tracking
- Key methods to replicate: `create_bucket`, `bucket_exists`, `test_connection`, `thaw`, `refreeze`, `list_objects`, `delete_bucket`, `put_object`, `list_buckets`, `head_object`, `copy_object`

**Backend logic to reference:**
- Factory pattern: `s3_client_factory(provider: str)` function that returns the appropriate client
- Credential validation on initialization: AWS validates by calling `list_buckets()` in `__init__`
- Error handling: Uses `ActionError` exception from `curator.exceptions`
- Logging: Uses standard Python logging with descriptive messages and progress tracking

## Visual Assets

### Files Provided:
No visual assets provided.

### Visual Insights:
N/A - No visual files found during mandatory check.

## Credential Validation Research

Based on research of GCP and Azure documentation, here are the recommended credential validation patterns:

### GCP Cloud Storage
**Validation Method**: `list_buckets()`

```python
from google.cloud import storage

storage_client = storage.Client()
# Validate credentials by listing buckets
buckets = list(storage_client.list_buckets())
```

**Key Points:**
- GCP uses Application Default Credentials (ADC)
- The Client() initialization is lazy - credentials aren't validated until an API call
- `list_buckets()` is the equivalent to AWS's pattern
- Will raise authentication errors if credentials are invalid
- Credentials can be provided via:
  - Service account JSON file (GOOGLE_APPLICATION_CREDENTIALS env var)
  - `gcloud auth application-default login` for local development
  - Workload Identity for GKE
  - Compute Engine service accounts

### Azure Blob Storage
**Validation Method**: `list_containers()`

```python
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(
    account_url="https://<account>.blob.core.windows.net",
    credential=credential
)
# Validate credentials by listing containers
containers = blob_service_client.list_containers()
```

**Key Points:**
- Azure uses `DefaultAzureCredential` for passwordless authentication
- Client initialization is lazy - credentials aren't validated until an API call
- `list_containers()` is the equivalent to AWS's `list_buckets()`
- Will raise authentication errors if credentials are invalid
- Returns a generator that follows continuation tokens
- Credentials can be provided via:
  - Connection string (includes account key)
  - Managed Identity (for Azure VMs/services)
  - Service Principal (client ID/secret)
  - Azure CLI authentication (`az login`)
  - Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)

## Requirements Summary

### Functional Requirements

**Core Functionality:**
1. Implement GcpStorageClient class that extends S3Client abstract base class
2. Implement AzureBlobClient class that extends S3Client abstract base class
3. Both clients must implement all abstract methods from S3Client
4. Credential validation on initialization matching AWS pattern (GCP: list_buckets, Azure: list_containers)
5. Support for cold storage classes:
   - GCP: Archive and Coldline
   - Azure: Archive tier
6. Support for retrieval/rehydration tiers:
   - GCP: Standard retrieval (no tier options like AWS)
   - Azure: Standard and High priority rehydration
7. Configuration via deepfreeze section in curator.yml

**User Actions Enabled:**
- Initialize deepfreeze with GCP or Azure provider via `--provider gcp` or `--provider azure`
- Create buckets/containers in GCP or Azure
- Snapshot and archive Elasticsearch indices to GCP Cloud Storage or Azure Blob Storage
- Restore (thaw) archived data from GCP Archive/Coldline or Azure Archive tier
- Refreeze restored data back to cold storage
- List, delete, and manage objects in GCP or Azure storage
- Configure storage class mappings per provider in curator.yml

**Data to be Managed:**
- Elasticsearch snapshot data stored in provider-specific cold storage tiers
- Repository metadata and configuration
- Thaw request state and progress tracking
- Storage class mappings in configuration

### Configuration Structure

**New deepfreeze section in curator.yml:**

```yaml
elasticsearch:
  # ... existing elasticsearch configuration

logging:
  # ... existing logging configuration

deepfreeze:
  provider: aws  # or gcp, or azure

  aws:
    region: us-west-2
    storage_class_map:
      cold: GLACIER
      deep_cold: DEEP_ARCHIVE
      intelligent: INTELLIGENT_TIERING
    retrieval_tier: Standard  # Standard, Expedited, or Bulk

  gcp:
    location: us-west1  # or multi-region like 'us'
    storage_class_map:
      cold: COLDLINE
      deep_cold: ARCHIVE
    # GCP doesn't have retrieval tiers like AWS

  azure:
    location: westus2
    storage_class_map:
      cold: Cool
      deep_cold: Archive
    rehydration_priority: Standard  # Standard or High
```

### Reusability Opportunities

**Components that exist and can be reused:**
1. **Abstract Base Class Pattern**: The existing S3Client abstract base class defines the complete interface
2. **Factory Pattern**: The `s3_client_factory()` function just needs two new branches
3. **Error Handling**: Use existing `ActionError` exception from `curator.exceptions`
4. **Logging Pattern**: Follow the existing logging approach with progress tracking
5. **Method Signatures**: All abstract methods are already defined with proper type hints

**Backend patterns to follow:**
1. **Credential Validation**: Initialize client, attempt list operation, catch auth errors with helpful messages
2. **Progress Tracking**: Log operation start, count successes/failures, log summary (see `thaw` and `refreeze` methods)
3. **Pagination**: Handle provider-specific pagination for listing operations
4. **Error Messages**: Provide actionable error messages with specific error codes when possible

**Similar logic to reference:**
1. **Region Handling**: AWS's bucket creation handles region-specific requirements (line 246-259 in s3client.py)
2. **Storage Class Filtering**: AWS's thaw method filters objects by storage class (line 343-368)
3. **Batch Operations**: AWS's refreeze method uses pagination to process all objects (line 415-454)

### Scope Boundaries

**In Scope:**
- GCP Cloud Storage client implementation (GcpStorageClient)
- Azure Blob Storage client implementation (AzureBlobClient)
- Credential validation on initialization for both providers
- All abstract methods from S3Client implemented for both providers
- Configuration mapping for storage classes in curator.yml
- Optional dependency installation via pip extras
- Clear error messages when optional dependencies are missing
- Factory pattern extension to support 'gcp' and 'azure' providers
- Documentation in examples/curator.yml showing new structure
- Unit tests with mocked provider APIs
- Integration tests against live GCP and Azure environments

**Out of Scope:**
- Cross-provider migration (covered in roadmap item #4)
- Enhanced setup validation for provider quotas/billing (roadmap item #5)
- Cost reporting and optimization (roadmap item #6)
- Parallel thaw operations (roadmap item #7)
- Smart retrieval tier selection (roadmap item #8)
- Retention policy automation (roadmap item #9)
- Monitoring/observability (roadmap item #10)
- Repository health checks (roadmap item #11)
- Compression optimization (roadmap item #12)
- Changes to the deepfreeze action workflow itself (setup, thaw, refreeze commands remain unchanged)
- UI/CLI changes beyond what's needed to support provider selection

**Future Enhancements Mentioned:**
- Multi-cloud cost comparison and optimization
- Automated provider selection based on cost/performance requirements
- Provider failover and redundancy
- Cross-provider data replication

### Technical Considerations

**Integration Points:**
- `curator/s3client.py`: Add GcpStorageClient and AzureBlobClient classes
- `curator/s3client.py`: Update `s3_client_factory()` to support 'gcp' and 'azure'
- Configuration parsing: Extend to handle deepfreeze section in curator.yml
- Schema validation: Update to validate deepfreeze configuration structure
- CLI: Existing `--provider` option should work with new providers

**Existing System Constraints:**
- Must maintain backward compatibility with existing AWS-only configurations
- Must not break existing AwsS3Client functionality
- Abstract method signatures cannot change (would break existing code)
- Error handling must use existing ActionError exception
- Logging must use standard Python logging module

**Technology Preferences:**
- Use `google-cloud-storage` library for GCP (official Google SDK)
- Use `azure-storage-blob` library for Azure (official Microsoft SDK)
- Follow pythonic patterns for each provider's SDK
- Make these optional dependencies via pip extras
- Prefer provider-native authentication methods (ADC for GCP, DefaultAzureCredential for Azure)

**Similar Code Patterns to Follow:**
- **Client Initialization with Validation**: Lines 198-222 in AwsS3Client.__init__
  - Initialize SDK client
  - Attempt validation operation (list_buckets/list_containers)
  - Catch provider-specific auth errors
  - Raise ActionError with helpful message
  - Log success/failure appropriately

- **Region/Location Handling**: Lines 246-259 in AwsS3Client.create_bucket
  - Get region from client configuration
  - Handle provider-specific region requirements
  - Pass appropriate location parameters

- **Pagination Pattern**: Lines 415-421 in AwsS3Client.refreeze
  - Use provider's paginator
  - Iterate through pages
  - Process contents of each page
  - Track progress across pages

- **Error Context**: Throughout AwsS3Client
  - Wrap provider exceptions in ActionError
  - Include operation context in error messages
  - Log errors with exc_info=True when appropriate

## Implementation Architecture

### Class Structure

```
S3Client (Abstract Base Class) - curator/s3client.py
├── AwsS3Client (Existing)
├── GcpStorageClient (New)
└── AzureBlobClient (New)

s3_client_factory(provider: str) -> S3Client
├── if provider == "aws": return AwsS3Client()
├── if provider == "gcp": return GcpStorageClient()
└── if provider == "azure": return AzureBlobClient()
```

### Method Implementation Priority

**High Priority (Core Operations):**
1. `__init__` - Client initialization with credential validation
2. `test_connection` - Verify connectivity and credentials
3. `create_bucket` - Create storage container
4. `bucket_exists` - Check if container exists
5. `list_buckets` - List all containers (for validation)

**Medium Priority (Data Operations):**
6. `thaw` - Restore from cold storage
7. `refreeze` - Move back to cold storage
8. `list_objects` - List objects with prefix
9. `head_object` - Get object metadata

**Lower Priority (Management Operations):**
10. `delete_bucket` - Delete container
11. `put_object` - Upload object
12. `copy_object` - Copy with storage class change

### Storage Class Mappings

**Provider-Specific Storage Classes:**

| Curator Config | AWS | GCP | Azure |
|----------------|-----|-----|-------|
| cold | GLACIER | COLDLINE | Cool |
| deep_cold | DEEP_ARCHIVE | ARCHIVE | Archive |
| intelligent | INTELLIGENT_TIERING | - | - |
| standard | STANDARD | STANDARD | Hot |

**Retrieval/Rehydration Options:**

| Curator Config | AWS | GCP | Azure |
|----------------|-----|-----|-------|
| Standard (default) | Standard | N/A | Standard |
| Fast/High | Expedited | N/A | High |
| Bulk | Bulk | N/A | N/A |

### Testing Strategy

**Unit Tests (Mocked APIs):**
- Mock provider SDK clients (boto3, google-cloud-storage, azure-storage-blob)
- Test all method implementations with various scenarios:
  - Success cases
  - Authentication failures
  - Permission errors
  - Not found errors
  - Network errors
- Test factory pattern with all providers
- Test missing optional dependency handling
- Test configuration parsing and validation

**Integration Tests (Live Credentials):**
- Test against real GCP project (requires service account)
- Test against real Azure storage account (requires credentials)
- Environment variables for test credentials:
  - GCP: `GOOGLE_APPLICATION_CREDENTIALS`
  - Azure: `AZURE_STORAGE_CONNECTION_STRING` or managed identity
- Test full workflow: setup → create bucket → deepfreeze → thaw → refreeze → cleanup
- Parallel tests for AWS, GCP, and Azure (if credentials available)

**Test Coverage Requirements:**
- Minimum 80% code coverage for new client classes
- 100% coverage of abstract method implementations
- All error paths tested

### Files to Create/Modify

**New Files:**
- None (all changes in existing files)

**Files to Modify:**

1. `/Users/bret/git/curator/curator/s3client.py`
   - Add `GcpStorageClient` class
   - Add `AzureBlobClient` class
   - Update `s3_client_factory()` function
   - Add import guards for optional dependencies

2. `/Users/bret/git/curator/examples/curator.yml`
   - Add deepfreeze section with provider configurations
   - Document storage class mappings
   - Show examples for AWS, GCP, and Azure

3. `/Users/bret/git/curator/pyproject.toml`
   - Add optional dependencies:
     ```toml
     [project.optional-dependencies]
     gcp = ["google-cloud-storage>=2.0.0"]
     azure = ["azure-storage-blob>=12.0.0", "azure-identity>=1.12.0"]
     all = ["google-cloud-storage>=2.0.0", "azure-storage-blob>=12.0.0", "azure-identity>=1.12.0"]
     ```

4. Configuration parsing code (exact location TBD - likely in curator/actions/deepfreeze/)
   - Parse deepfreeze section from curator.yml
   - Validate provider-specific configuration
   - Pass configuration to client initialization

5. Schema validation files (exact location TBD)
   - Add schema for deepfreeze configuration section
   - Validate storage_class_map structure
   - Validate provider-specific options

6. Test files (to be created in appropriate test directories)
   - `test_gcp_storage_client.py` - Unit tests for GcpStorageClient
   - `test_azure_blob_client.py` - Unit tests for AzureBlobClient
   - `test_s3_client_factory.py` - Tests for factory pattern
   - Integration test files for live API testing

### Configuration Change Summary

**Current Configuration (AWS-only, implicit):**
- AWS credentials via environment variables or IAM roles
- No explicit storage class configuration
- Hard-coded storage classes in code

**New Configuration (Multi-cloud, explicit):**
- Provider selection in curator.yml
- Provider-specific sections with mappings
- Storage class configuration per provider
- Region/location configuration per provider
- Retrieval tier defaults per provider

**Migration Path:**
- Existing configurations work unchanged (AWS remains default)
- New deepfreeze section is optional
- If not specified, falls back to AWS with existing behavior
- Users can opt-in to explicit configuration incrementally

## Success Criteria

**Feature Complete When:**
1. GcpStorageClient implements all S3Client abstract methods
2. AzureBlobClient implements all S3Client abstract methods
3. Both clients validate credentials on initialization
4. Factory pattern supports 'gcp' and 'azure' providers
5. Configuration parsing handles deepfreeze section
6. Optional dependencies work correctly with pip extras
7. Clear error messages for missing dependencies
8. Unit tests achieve 80%+ coverage
9. Integration tests pass against live GCP and Azure
10. Documentation updated in examples/curator.yml
11. Existing AWS functionality remains unchanged

**Definition of Done:**
- All abstract methods implemented and tested
- Code passes black, ruff, and pyright checks
- All tests pass (unit and integration)
- PR approved and merged
- Documentation updated
- Backward compatibility verified
