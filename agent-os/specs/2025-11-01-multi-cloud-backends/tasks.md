# Task Breakdown: Multi-Cloud Backend Support for Deepfreeze

## Overview
Total Tasks: 58 sub-tasks organized into 6 major task groups

This implementation adds Google Cloud Platform (GCP) and Microsoft Azure support to Elasticsearch Curator's deepfreeze functionality, expanding from AWS-only to multi-cloud architecture.

## Task List

### Configuration Layer

#### Task Group 1: Configuration Schema and Validation
**Dependencies:** None

- [ ] 1.0 Complete configuration infrastructure
  - [ ] 1.1 Write 2-5 focused tests for configuration parsing
    - Test deepfreeze section parsing from curator.yml
    - Test provider selection validation (aws, gcp, azure)
    - Test storage_class_map structure validation
    - Test region/location configuration parsing
    - Test backward compatibility (missing deepfreeze section defaults to AWS)
  - [ ] 1.2 Update examples/curator.yml with deepfreeze section
    - Add deepfreeze section with provider field
    - Add aws subsection with region, storage_class_map, retrieval_tier
    - Add gcp subsection with location, storage_class_map
    - Add azure subsection with location, storage_class_map, rehydration_priority
    - Include comments documenting each provider's options
    - Reference: /Users/bret/git/curator/examples/curator.yml (lines 1-33)
  - [ ] 1.3 Update schema validation in validators
    - Add deepfreeze section to schema in curator/validators/options.py
    - Define provider as Required field with values: [aws, gcp, azure]
    - Define aws subsection schema (region, storage_class_map dict, retrieval_tier)
    - Define gcp subsection schema (location, storage_class_map dict)
    - Define azure subsection schema (location, storage_class_map dict, rehydration_priority)
    - Use voluptuous Schema patterns from existing validators
    - Reference: /Users/bret/git/curator/curator/validators/options.py (lines 1-50)
  - [ ] 1.4 Extend Settings dataclass in helpers.py
    - Add deepfreeze_config: dict field to Settings dataclass
    - Add parsing logic to load deepfreeze section from curator.yml
    - Add method to get storage_class_map for current provider
    - Add method to get region/location for current provider
    - Add method to get retrieval/rehydration tier for current provider
    - Maintain backward compatibility (default to AWS if not specified)
    - Reference: /Users/bret/git/curator/curator/actions/deepfreeze/helpers.py (lines 1-100)
  - [ ] 1.5 Ensure configuration tests pass
    - Run ONLY the 2-5 tests written in 1.1
    - Verify examples/curator.yml is valid YAML
    - Verify schema validation accepts valid configurations
    - Do NOT run the entire test suite at this stage

**Acceptance Criteria:**
- The 2-5 tests written in 1.1 pass
- examples/curator.yml contains documented deepfreeze section
- Schema validation enforces provider selection and structure
- Settings dataclass can access provider-specific configuration
- Backward compatibility maintained (existing configs work unchanged)

### Dependency Management

#### Task Group 2: Optional Dependencies and Import Guards
**Dependencies:** None (can run in parallel with Task Group 1)

- [ ] 2.0 Complete dependency management
  - [ ] 2.1 Write 2-4 focused tests for optional dependency handling
    - Test factory raises clear error when GCP dependency missing
    - Test factory raises clear error when Azure dependency missing
    - Test error messages include installation instructions
    - Test AWS works without optional dependencies installed
  - [ ] 2.2 Update pyproject.toml with optional dependencies
    - Add [project.optional-dependencies] section
    - Add gcp = ["google-cloud-storage>=2.0.0"]
    - Add azure = ["azure-storage-blob>=12.0.0", "azure-identity>=1.12.0"]
    - Add all = ["google-cloud-storage>=2.0.0", "azure-storage-blob>=12.0.0", "azure-identity>=1.12.0"]
    - Keep boto3 in main dependencies for backward compatibility
    - Reference: /Users/bret/git/curator/pyproject.toml
  - [ ] 2.3 Add import guards to s3client.py
    - Add try/except blocks for google-cloud-storage imports
    - Add try/except blocks for azure-storage-blob imports
    - Set module-level flags (HAS_GCP, HAS_AZURE) based on import success
    - Add helper function to check dependencies before client creation
    - Reference existing boto3 imports in /Users/bret/git/curator/curator/s3client.py (lines 1-20)
  - [ ] 2.4 Update s3_client_factory() with dependency checks
    - Check HAS_GCP before creating GcpStorageClient
    - Check HAS_AZURE before creating AzureBlobClient
    - Raise ActionError with installation instructions if missing
    - Format: "GCP support requires: pip install elasticsearch-curator[gcp]"
    - Format: "Azure support requires: pip install elasticsearch-curator[azure]"
    - Reference: /Users/bret/git/curator/curator/exceptions.py for ActionError
  - [ ] 2.5 Ensure dependency management tests pass
    - Run ONLY the 2-4 tests written in 2.1
    - Verify pyproject.toml is valid TOML
    - Verify import guards work correctly
    - Do NOT run the entire test suite at this stage

**Acceptance Criteria:**
- The 2-4 tests written in 2.1 pass
- pyproject.toml defines gcp, azure, and all extras
- Import guards prevent import errors
- Factory function provides clear error messages for missing dependencies
- AWS functionality works without optional dependencies

### GCP Client Implementation

#### Task Group 3: GCP Cloud Storage Client
**Dependencies:** Task Group 2 (needs import guards)

- [ ] 3.0 Complete GCP client implementation
  - [ ] 3.1 Write 2-8 focused tests for GcpStorageClient
    - Test credential validation on initialization (mocked list_buckets call)
    - Test create_bucket with region handling
    - Test bucket_exists check
    - Test thaw operation with Archive/Coldline filtering
    - Test refreeze operation with storage class mapping
    - Test list_objects with pagination
    - Mock google-cloud-storage SDK calls using unittest.mock
    - Skip exhaustive edge case testing
  - [ ] 3.2 Create GcpStorageClient class skeleton
    - Add class definition extending S3Client abstract base class
    - Add __init__ method signature with region/location parameter
    - Import google.cloud.storage and google.auth.exceptions
    - Add docstring explaining GCP-specific behavior
    - Reference: /Users/bret/git/curator/curator/s3client.py (AwsS3Client pattern)
  - [ ] 3.3 Implement GCP credential validation
    - Initialize storage.Client() with Application Default Credentials
    - Call storage_client.list_buckets() in __init__ to validate
    - Catch google.auth.exceptions.DefaultCredentialsError
    - Wrap in ActionError with helpful message about GOOGLE_APPLICATION_CREDENTIALS
    - Log success/failure appropriately
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.__init__ (lines 198-222)
  - [ ] 3.4 Implement GCP bucket operations
    - Implement create_bucket(bucket_name) mapping to bucket.create()
    - Handle location parameter (region or multi-region)
    - Implement bucket_exists(bucket_name) using bucket.exists()
    - Implement delete_bucket(bucket_name) mapping to bucket.delete()
    - Implement list_buckets() returning list of bucket names
    - Add error handling with ActionError wrapping
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client bucket methods
  - [ ] 3.5 Implement GCP object operations
    - Implement list_objects(bucket_name, prefix) using bucket.list_blobs()
    - Handle GCP-specific pagination with page_token
    - Implement head_object(bucket_name, key) using blob.reload()
    - Implement put_object(bucket_name, key, body) using blob.upload_from_string()
    - Implement copy_object(bucket_name, source_key, dest_key, storage_class)
    - Map storage_class to GCP classes (ARCHIVE, COLDLINE, STANDARD)
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client object methods
  - [ ] 3.6 Implement GCP thaw operation
    - Filter objects by storage_class matching ["ARCHIVE", "COLDLINE"]
    - Use blob.rewrite() to restore from cold storage to STANDARD
    - Iterate through object_keys list with progress tracking
    - Count restored/skipped/error objects
    - Log summary with counts at completion
    - Note: GCP doesn't have restoration expiry like AWS
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.thaw (lines 343-368)
  - [ ] 3.7 Implement GCP refreeze operation
    - List all objects under path prefix using pagination
    - Use blob.rewrite() to move to target storage_class
    - Process page by page with progress logging
    - Map storage_class from configuration (ARCHIVE or COLDLINE)
    - Count successes/failures and log summary
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.refreeze (lines 415-454)
  - [ ] 3.8 Implement GCP test_connection method
    - Call list_buckets() and return True on success
    - Catch and log any exceptions, return False on failure
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.test_connection
  - [ ] 3.9 Ensure GCP client tests pass
    - Run ONLY the 2-8 tests written in 3.1
    - Verify all abstract methods are implemented
    - Verify error handling works correctly
    - Do NOT run the entire test suite at this stage

**Acceptance Criteria:**
- The 2-8 tests written in 3.1 pass
- GcpStorageClient implements all 11 abstract methods from S3Client
- Credential validation follows AWS pattern
- Storage class mapping works for ARCHIVE and COLDLINE
- Pagination handles GCP-specific patterns
- Error messages are clear and actionable

### Azure Client Implementation

#### Task Group 4: Azure Blob Storage Client
**Dependencies:** Task Group 2 (needs import guards)

- [ ] 4.0 Complete Azure client implementation
  - [ ] 4.1 Write 2-8 focused tests for AzureBlobClient
    - Test credential validation on initialization (mocked list_containers call)
    - Test create_bucket (container) with location handling
    - Test bucket_exists (container_exists) check
    - Test thaw operation with Archive tier filtering
    - Test refreeze operation with access tier mapping
    - Test list_objects with continuation token pagination
    - Mock azure-storage-blob SDK calls using unittest.mock
    - Skip exhaustive edge case testing
  - [ ] 4.2 Create AzureBlobClient class skeleton
    - Add class definition extending S3Client abstract base class
    - Add __init__ method with account_url and location parameters
    - Import azure.storage.blob and azure.identity
    - Add docstring explaining Azure-specific behavior and terminology (bucket=container)
    - Reference: /Users/bret/git/curator/curator/s3client.py (AwsS3Client pattern)
  - [ ] 4.3 Implement Azure credential validation
    - Initialize DefaultAzureCredential() for authentication
    - Create BlobServiceClient with account_url and credential
    - Call blob_service_client.list_containers() in __init__ to validate
    - Catch azure.core.exceptions.ClientAuthenticationError
    - Wrap in ActionError with helpful message about Azure CLI or service principal
    - Log success/failure appropriately
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.__init__ (lines 198-222)
  - [ ] 4.4 Implement Azure container operations (bucket equivalents)
    - Implement create_bucket(bucket_name) mapping to create_container()
    - Handle location via account_url (e.g., https://account.westus2.blob.core.windows.net)
    - Implement bucket_exists(bucket_name) using get_container_client().exists()
    - Implement delete_bucket(bucket_name) mapping to delete_container()
    - Implement list_buckets() by iterating list_containers() generator
    - Add error handling with ActionError wrapping
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client bucket methods
  - [ ] 4.5 Implement Azure blob operations (object equivalents)
    - Implement list_objects(bucket_name, prefix) using list_blobs(name_starts_with=prefix)
    - Handle Azure-specific continuation tokens for pagination
    - Implement head_object(bucket_name, key) using get_blob_properties()
    - Implement put_object(bucket_name, key, body) using upload_blob()
    - Implement copy_object(bucket_name, source_key, dest_key, storage_class)
    - Map storage_class to Azure access tiers (Archive, Cool, Hot)
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client object methods
  - [ ] 4.6 Implement Azure thaw operation (rehydration)
    - Filter blobs by access_tier matching ["Archive"]
    - Use set_blob_tier() to rehydrate from Archive to Hot or Cool
    - Set rehydration_priority from configuration (Standard or High)
    - Iterate through object_keys list with progress tracking
    - Count rehydrated/skipped/error blobs
    - Log summary with counts at completion
    - Note: Azure rehydration is asynchronous (similar to AWS Glacier restore)
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.thaw (lines 343-368)
  - [ ] 4.7 Implement Azure refreeze operation
    - List all blobs under path prefix using continuation token pagination
    - Use set_blob_tier() to move to target access tier
    - Process page by page with progress logging
    - Map storage_class from configuration (Archive or Cool)
    - Count successes/failures and log summary
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.refreeze (lines 415-454)
  - [ ] 4.8 Implement Azure test_connection method
    - Call list_containers() and return True on success
    - Catch and log any exceptions, return False on failure
    - Reference: /Users/bret/git/curator/curator/s3client.py AwsS3Client.test_connection
  - [ ] 4.9 Ensure Azure client tests pass
    - Run ONLY the 2-8 tests written in 4.1
    - Verify all abstract methods are implemented
    - Verify error handling works correctly
    - Do NOT run the entire test suite at this stage

**Acceptance Criteria:**
- The 2-8 tests written in 4.1 pass
- AzureBlobClient implements all 11 abstract methods from S3Client
- Credential validation follows AWS pattern
- Storage class mapping works for Archive and Cool tiers
- Pagination handles Azure-specific continuation tokens
- Rehydration priority (Standard/High) is configurable
- Error messages are clear and actionable

### Factory Pattern and Integration

#### Task Group 5: Factory Pattern and Provider Selection
**Dependencies:** Task Groups 3 and 4 (needs client implementations)

- [ ] 5.0 Complete factory pattern integration
  - [ ] 5.1 Write 2-6 focused tests for factory pattern
    - Test s3_client_factory("aws") returns AwsS3Client
    - Test s3_client_factory("gcp") returns GcpStorageClient
    - Test s3_client_factory("azure") returns AzureBlobClient
    - Test factory raises error for invalid provider
    - Test factory raises error when GCP dependency missing
    - Test factory raises error when Azure dependency missing
  - [ ] 5.2 Update s3_client_factory() function
    - Add "gcp" branch that checks HAS_GCP and returns GcpStorageClient()
    - Add "azure" branch that checks HAS_AZURE and returns AzureBlobClient()
    - Maintain existing "aws" branch returning AwsS3Client()
    - Add dependency check before instantiation
    - Raise ActionError with installation instructions if dependency missing
    - Add default case raising ActionError for invalid provider string
    - Reference: Current s3_client_factory() in /Users/bret/git/curator/curator/s3client.py
  - [ ] 5.3 Update deepfreeze actions to use configuration
    - Update Setup class to pass provider from Settings to factory
    - Update Thaw class to pass provider from Settings to factory
    - Update Cleanup class to pass provider from Settings to factory
    - Update Rotate class to pass provider from Settings to factory
    - Update Status class to pass provider from Settings to factory
    - Update RepairMetadata class to pass provider from Settings to factory
    - Verify existing calls like s3_client_factory(self.settings.provider) work unchanged
    - Reference: Existing usage in /Users/bret/git/curator/curator/actions/deepfreeze/*.py
  - [ ] 5.4 Verify provider configuration flow
    - Verify Settings.provider defaults to "aws" for backward compatibility
    - Verify Settings loads provider from deepfreeze section when present
    - Verify factory receives correct provider string from Settings
    - Verify client initialization receives correct configuration (region/location)
    - Test complete flow: config → Settings → factory → client
  - [ ] 5.5 Ensure factory integration tests pass
    - Run ONLY the 2-6 tests written in 5.1
    - Verify factory returns correct client type for each provider
    - Verify error handling works correctly
    - Do NOT run the entire test suite at this stage

**Acceptance Criteria:**
- The 2-6 tests written in 5.1 pass
- Factory pattern supports aws, gcp, and azure providers
- Factory provides clear error messages for invalid providers
- Factory checks dependencies before instantiation
- All deepfreeze actions use factory correctly
- Backward compatibility maintained (defaults to AWS)

### Testing and Validation

#### Task Group 6: Integration Testing and Validation
**Dependencies:** Task Groups 1-5 (needs complete implementation)

- [ ] 6.0 Complete integration testing and validation
  - [ ] 6.1 Review existing tests and assess coverage gaps
    - Review tests from Task Group 1 (configuration - 2-5 tests)
    - Review tests from Task Group 2 (dependencies - 2-4 tests)
    - Review tests from Task Group 3 (GCP client - 2-8 tests)
    - Review tests from Task Group 4 (Azure client - 2-8 tests)
    - Review tests from Task Group 5 (factory - 2-6 tests)
    - Total existing tests: approximately 10-31 unit tests
    - Identify critical integration test gaps
  - [ ] 6.2 Write integration tests for AWS (baseline)
    - Test complete AWS deepfreeze workflow with real credentials
    - Verify backward compatibility with existing AWS configurations
    - Test: setup → create bucket → snapshot → thaw → refreeze → cleanup
    - Use environment variable for AWS credentials (existing pattern)
    - Maximum 2-3 integration tests for AWS baseline
  - [ ] 6.3 Write integration tests for GCP
    - Test complete GCP deepfreeze workflow with real credentials
    - Verify GCP-specific storage classes (ARCHIVE, COLDLINE)
    - Test: setup → create bucket → snapshot → thaw → refreeze → cleanup
    - Use GOOGLE_APPLICATION_CREDENTIALS environment variable
    - Maximum 2-3 integration tests for GCP
    - Skip if GCP credentials not available (mark as pytest.skip)
  - [ ] 6.4 Write integration tests for Azure
    - Test complete Azure deepfreeze workflow with real credentials
    - Verify Azure-specific access tiers (Archive, Cool)
    - Test rehydration priority configuration (Standard, High)
    - Test: setup → create container → snapshot → thaw → refreeze → cleanup
    - Use Azure credential environment variables or DefaultAzureCredential
    - Maximum 2-3 integration tests for Azure
    - Skip if Azure credentials not available (mark as pytest.skip)
  - [ ] 6.5 Write end-to-end workflow tests
    - Test configuration loading from curator.yml for each provider
    - Test storage class mapping resolution from configuration
    - Test region/location handling for each provider
    - Test retrieval/rehydration tier configuration
    - Maximum 3-4 end-to-end workflow tests
  - [ ] 6.6 Run feature-specific test suite
    - Run all unit tests from Task Groups 1-5 (approximately 10-31 tests)
    - Run integration tests from Task Group 6 (approximately 7-13 tests)
    - Expected total: approximately 17-44 tests for this feature
    - Do NOT run the entire application test suite
    - Verify all feature-specific tests pass
  - [ ] 6.7 Validate code quality and standards
    - Run black --check --diff on modified files
    - Run ruff check on modified files
    - Run pyright on modified files
    - Ensure 88-character line length compliance
    - Fix any linting or type checking errors
    - Reference: /Users/bret/git/curator/CLAUDE.md Code Quality section

**Acceptance Criteria:**
- All feature-specific tests pass (approximately 17-44 tests total)
- Integration tests cover AWS, GCP, and Azure workflows
- Code passes black, ruff, and pyright checks
- Test coverage for new code exceeds 80%
- End-to-end workflows validated for all three providers
- Backward compatibility with AWS verified

## Execution Order

Recommended implementation sequence:

1. **Configuration Layer (Task Group 1)** - Foundation for all provider configuration
2. **Dependency Management (Task Group 2)** - Can run in parallel with Task Group 1
3. **GCP Client (Task Group 3)** - First new provider implementation
4. **Azure Client (Task Group 4)** - Can run in parallel with Task Group 3
5. **Factory Integration (Task Group 5)** - Ties everything together
6. **Testing and Validation (Task Group 6)** - Final verification

## Key Files Modified

### Primary Implementation Files
- `/Users/bret/git/curator/curator/s3client.py` - Add GcpStorageClient and AzureBlobClient classes, update factory
- `/Users/bret/git/curator/curator/actions/deepfreeze/helpers.py` - Extend Settings dataclass with deepfreeze_config
- `/Users/bret/git/curator/examples/curator.yml` - Add deepfreeze section with provider examples
- `/Users/bret/git/curator/pyproject.toml` - Add optional dependencies for gcp, azure, all extras

### Schema and Validation Files
- `/Users/bret/git/curator/curator/validators/options.py` - Add deepfreeze schema validation

### Test Files (to be created)
- `tests/unit/test_gcp_storage_client.py` - Unit tests for GcpStorageClient
- `tests/unit/test_azure_blob_client.py` - Unit tests for AzureBlobClient
- `tests/unit/test_s3_client_factory.py` - Tests for factory pattern
- `tests/unit/test_deepfreeze_config.py` - Tests for configuration parsing
- `tests/integration/test_gcp_integration.py` - GCP integration tests
- `tests/integration/test_azure_integration.py` - Azure integration tests
- `tests/integration/test_aws_backward_compat.py` - AWS backward compatibility tests

## Implementation Notes

### Testing Philosophy
- Each task group writes 2-8 focused unit tests during development
- Testing focuses on critical behaviors, not exhaustive coverage
- Integration tests validate end-to-end workflows with real APIs
- Maximum of 10 additional tests added during Task Group 6 if needed
- Total expected tests for feature: approximately 17-44 tests

### Backward Compatibility Requirements
- Existing AWS-only configurations must work unchanged
- Default provider is "aws" when deepfreeze section is missing
- boto3 remains a required dependency (not optional)
- No breaking changes to S3Client abstract interface
- All existing deepfreeze actions continue to work

### Storage Class Mapping Strategy
- Configuration-driven mappings in curator.yml deepfreeze section
- Logical names (cold, deep_cold, intelligent, standard) map to provider-specific classes
- AWS: GLACIER, DEEP_ARCHIVE, INTELLIGENT_TIERING, STANDARD
- GCP: COLDLINE, ARCHIVE, STANDARD
- Azure: Cool, Archive, Hot

### Error Handling Patterns
- Use ActionError exception for all failures (from curator.exceptions)
- Provide clear, actionable error messages with context
- Include installation instructions for missing optional dependencies
- Log errors with exc_info=True for unexpected failures
- Follow existing AWS error handling patterns

### Credential Validation Patterns
- Validate credentials immediately on client initialization
- Use provider-native validation calls (list_buckets, list_containers)
- Catch provider-specific authentication exceptions
- Wrap in ActionError with helpful setup instructions
- Log validation success/failure appropriately

## Success Metrics

**Feature Complete When:**
1. All 6 task groups completed with acceptance criteria met
2. Approximately 17-44 feature-specific tests written and passing
3. GcpStorageClient and AzureBlobClient implement all 11 abstract methods
4. Factory pattern supports aws, gcp, and azure providers
5. Configuration parsing handles deepfreeze section correctly
6. Optional dependencies work via pip extras ([gcp], [azure], [all])
7. Integration tests pass against live GCP and Azure (when credentials available)
8. Code passes black, ruff, and pyright quality checks
9. examples/curator.yml documents new configuration structure
10. Backward compatibility verified with existing AWS configurations

**Definition of Done:**
- All task groups completed
- All tests passing (unit and integration)
- Code quality checks passing (black, ruff, pyright)
- Documentation updated (examples/curator.yml)
- PR ready for review
- Backward compatibility maintained
