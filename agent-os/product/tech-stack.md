# Tech Stack

## Language & Runtime
- **Python**: 3.8+ (supports up to 3.12)
  - Modern Python features with backward compatibility
  - Type hints for better code quality and IDE support

## Build & Packaging
- **Build System**: Hatchling
  - Modern Python packaging based on PEP 517/660
  - Simplified dependency management and build configuration
- **Package Manager**: pip
  - Standard Python package installation
  - Development mode installation (`pip install -e .`) for local development

## Core Dependencies

### Elasticsearch Integration
- **es_client**: 8.17.5
  - Official Elasticsearch client wrapper
  - Connection management and authentication
  - Handles API versioning and compatibility

### Cloud Storage
- **boto3**: Latest
  - AWS SDK for Python
  - S3 and Glacier API integration
  - IAM credential management and authentication
- **google-cloud-storage**: TBD (Planned for GCP backend)
  - Google Cloud Storage client library
  - Archive and Coldline tier support
- **azure-storage-blob**: TBD (Planned for Azure backend)
  - Azure Blob Storage client library
  - Archive tier and rehydration support

### CLI & User Interface
- **click**: Latest
  - Command-line interface framework
  - Three entry points: curator, curator_cli, es_repo_mgr
- **rich**: Latest
  - Rich text formatting in terminal
  - Progress bars, tables, and panels
  - Syntax highlighting and pretty printing

### Configuration
- **PyYAML**: Latest
  - YAML configuration file parsing
  - Support for elasticsearch and logging sections
  - Schema validation integration

## Testing

### Test Framework
- **pytest**: Latest
  - Primary testing framework
  - Test discovery and execution
  - Fixture support for test isolation
- **pytest-cov**: Latest
  - Code coverage reporting
  - Integration with pytest
  - Coverage threshold enforcement

### Test Requirements
- **Live Elasticsearch Instance**: Required for integration tests
  - Default target: localhost:9200
  - Configurable via TEST_ES_SERVER environment variable
  - WARNING: Tests DELETE ALL DATA in target instance
- **AWS Credentials**: Required for S3/Glacier tests
  - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  - IAM permissions for S3 operations
- **GCP Service Account**: TBD (Planned for GCP tests)
  - Service account JSON credentials
  - Cloud Storage permissions
- **Azure Connection String**: TBD (Planned for Azure tests)
  - Storage account connection string or managed identity
  - Blob Storage permissions

## Code Quality

### Linting & Formatting
- **black**: Latest
  - Code formatter with 88-character line length
  - Consistent code style enforcement
  - Pre-commit hook integration
- **ruff**: Latest
  - Fast Python linter
  - Replaces flake8, isort, and others
  - Configurable rule sets
- **mypy**: Latest (optional)
  - Static type checking
  - Type hint validation
  - Gradual typing support
- **pyright**: Latest (via hatch lint environment)
  - Microsoft's Python type checker
  - Fast incremental type checking

### Quality Commands
```bash
# Via hatch (recommended)
hatch run lint:python        # Run all linters
hatch run lint:run-black     # Black formatting
hatch run lint:run-ruff      # Ruff linting

# Direct commands
black --check --diff .
ruff check .
mypy .
```

## Development Tools

### Environment Management
- **hatch**: Latest
  - Project environment manager
  - Script execution and task running
  - Virtual environment automation

### Docker Support
- **Docker**: Latest
  - Containerized development and testing
  - Multi-stage builds for binary creation
- **Docker Compose**: Latest (optional)
  - Multi-container test environments
  - Elasticsearch cluster setup

### Docker Commands
```bash
hatch run docker:create      # Create test environment
hatch run docker:destroy     # Destroy test environment
```

## Architecture Components

### Storage Abstraction Layer
- **S3Client (Abstract Base Class)**
  - Defines common interface for all cloud storage providers
  - Methods: create_bucket, bucket_exists, thaw, refreeze, list_objects, etc.
  - Provider-agnostic design for multi-cloud support

- **AwsS3Client (AWS Implementation)**
  - Implements S3Client for AWS S3/Glacier
  - Uses boto3 for AWS API calls
  - Handles region configuration and credential validation

- **GcpStorageClient (Planned)**
  - Will implement S3Client for GCP Cloud Storage
  - Archive and Coldline storage class support

- **AzureBlobClient (Planned)**
  - Will implement S3Client for Azure Blob Storage
  - Archive tier and rehydration support

### Action Layer
- **Setup**: Initial configuration and resource creation
- **Deepfreeze**: Snapshot creation and archival to cold storage
- **Thaw**: Restore from cold storage with sync/async modes
- **Refreeze**: Return restored data to cold storage
- **Rotate**: Repository rotation based on time/size
- **Cleanup**: Retention policy enforcement
- **Status**: Progress monitoring and state management
- **Repair Metadata**: Consistency checking and repair

### Data Layer
- **IndexList**: Elasticsearch index management and filtering
- **SnapshotList**: Snapshot enumeration and selection
- **Repository Objects**: State tracking for snapshot repositories
- **Thaw Requests**: Asynchronous operation tracking
- **Settings**: Deepfreeze configuration persistence

## Configuration Format

### YAML Structure
```yaml
elasticsearch:
  client:
    hosts: ['localhost:9200']
    # Other connection settings
  other_settings:
    # Curator-specific settings

logging:
  loglevel: INFO
  logfile: /path/to/curator.log
  # Other logging configuration
```

## Deployment

### Installation Methods
- **PyPI Package**: `pip install elasticsearch-curator`
- **Source Installation**: `pip install -e .` for development
- **Docker Container**: Pre-built images with all dependencies

### Runtime Requirements
- Python 3.8 or higher
- Network access to Elasticsearch cluster
- Cloud provider credentials (AWS/GCP/Azure)
- Sufficient disk space for snapshot operations

## Future Tech Stack Additions

### Planned Integrations
- **prometheus-client**: Metrics export for observability
- **grafana**: Dashboard integration for monitoring
- **celery**: Distributed task queue for parallel operations (optional)
- **redis**: Task queue backend and caching (optional)

### Performance Optimization
- **asyncio**: Asynchronous I/O for concurrent cloud operations
- **multiprocessing**: Parallel processing for large-scale thaw/refreeze
- **caching**: Local caching of repository metadata and status

### Security Enhancements
- **cryptography**: Encryption at rest for sensitive configuration
- **keyring**: Secure credential storage integration
- **audit logging**: Comprehensive operation audit trail
