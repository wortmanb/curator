# Product Mission

## Pitch
Elasticsearch Curator Deepfreeze is a data lifecycle management tool that helps DevOps engineers and data teams reduce long-term Elasticsearch storage costs by seamlessly archiving cold data to cloud object storage (AWS S3 Glacier, with planned support for GCP and Azure). By automating the freeze, thaw, and lifecycle management of Elasticsearch indices to cost-effective cloud storage tiers, teams can retain years of historical data without breaking the budget.

## Users

### Primary Customers
- **DevOps Engineers**: Managing Elasticsearch clusters with long-term retention requirements
- **Data Engineering Teams**: Organizations generating high volumes of time-series data (logs, metrics, events)
- **Enterprise IT**: Companies with compliance requirements needing multi-year data retention
- **Cost-Conscious Organizations**: Teams looking to optimize infrastructure spending while maintaining data accessibility

### User Personas

**Sarah - Senior DevOps Engineer** (32-45)
- **Role:** Infrastructure lead at a SaaS company
- **Context:** Manages a 50-node Elasticsearch cluster storing 6+ months of application logs and metrics
- **Pain Points:** Storage costs growing 20% monthly, pressure to reduce infrastructure spend while maintaining 3-year retention for compliance
- **Goals:** Archive data older than 90 days to reduce costs by 70%, restore historical data on-demand for incident investigation

**Marcus - Data Platform Engineer** (28-38)
- **Role:** Data infrastructure architect at a financial services firm
- **Context:** Oversees multiple Elasticsearch clusters across dev, staging, and production environments
- **Pain Points:** Vendor lock-in concerns with single cloud provider, complex manual processes for data archival, inconsistent tooling across teams
- **Goals:** Implement automated archival workflows, support multi-cloud strategy, simplify operational procedures for junior team members

## The Problem

### Expensive Long-Term Storage
Keeping all Elasticsearch data in hot/warm tiers is prohibitively expensive. A typical 10TB cluster storing 2 years of data can cost $5,000-15,000/month in cloud storage alone. Most of this data is rarely accessed after 30-90 days, yet teams keep it online "just in case."

**Our Solution:** Deepfreeze automatically archives cold data to AWS S3 Glacier (and soon GCP/Azure), reducing storage costs by 70-90% while maintaining the ability to restore data on-demand. Organizations can extend retention from months to years without proportional cost increases.

### Complex Setup and Configuration
Setting up deep archival requires configuring S3 buckets, snapshot repositories, IAM policies, Elasticsearch plugins, and ILM policies. Teams spend hours troubleshooting configuration issues, missing dependencies, and permission problems. A single misconfiguration can lead to failed snapshots or inaccessible data.

**Our Solution:** Deepfreeze provides a guided setup process that validates all prerequisites, creates properly configured resources, and provides clear error messages with actionable solutions. The setup command handles bucket creation, repository configuration, and validation checks automatically.

### Single Cloud Provider Lock-In
Current solutions only support AWS S3, forcing teams into vendor lock-in. Organizations with multi-cloud strategies or those using GCP/Azure can't leverage their existing cloud investments for Elasticsearch archival.

**Our Solution:** Deepfreeze is architected with a provider-agnostic storage abstraction layer. While AWS is currently supported, the roadmap includes GCP Cloud Storage and Azure Blob Storage backends, enabling teams to choose the cloud provider that best fits their strategy and pricing model.

## Differentiators

### Multi-Cloud Storage Backend Support (Planned)
Unlike Elasticsearch's native snapshot features which only support AWS S3, Deepfreeze is designed from the ground up to support multiple cloud providers. The S3Client abstraction layer enables easy integration with GCP Cloud Storage and Azure Blob Storage, giving teams flexibility to optimize costs across providers or meet regulatory requirements for data residency.

This results in reduced vendor lock-in, better negotiating leverage with cloud providers, and the ability to match storage location with data sovereignty requirements.

### Automated Setup Validation
Unlike manual snapshot repository setup which requires deep Elasticsearch and cloud storage expertise, Deepfreeze's setup command performs comprehensive precondition checks before making any changes. It validates S3 plugin installation, credential configuration, bucket availability, and repository state - all with clear, actionable error messages.

This results in faster onboarding, fewer configuration errors, and significantly reduced time-to-value. Teams can go from zero to archiving data in minutes instead of hours.

### Intelligent Thaw Workflow
Unlike basic restore operations that require manual coordination of multiple steps, Deepfreeze provides a unified thaw workflow that handles Glacier restoration requests, status monitoring, repository mounting, and index remounting - all in a single command. Teams can choose synchronous (wait for completion) or asynchronous (initiate and check later) modes.

This results in simpler operational procedures, reduced risk of incomplete restores, and faster access to historical data when needed for investigations or compliance requests.

## Key Features

### Core Features
- **Automated Setup**: One-command initialization that creates S3 buckets, snapshot repositories, and validates all prerequisites with comprehensive error checking and guided troubleshooting
- **Deepfreeze (Snapshot & Archive)**: Seamlessly create Elasticsearch snapshots and move them to cost-effective Glacier storage tiers, reducing storage costs by 70-90% compared to hot/warm tiers
- **Thaw (Restore)**: Restore archived data from Glacier to instant-access tiers with flexible sync/async modes, automatic status monitoring, and repository/index mounting
- **Status Tracking**: Monitor ongoing thaw requests with detailed progress tracking, including per-repository restoration status and automatic state management

### Lifecycle Management Features
- **Rotate**: Automatically rotate to new repositories based on configurable time periods (monthly, yearly) or storage capacity, preventing individual repositories from becoming unwieldy
- **Refreeze**: Return temporarily restored data back to Glacier storage after the restore window expires, optimizing storage costs for temporary data access
- **Cleanup**: Remove old repositories and archived data based on retention policies, ensuring compliance with data retention requirements while minimizing storage costs
- **Repair Metadata**: Fix inconsistencies in repository metadata that can occur during failed operations or cluster issues, maintaining data integrity

### Cloud Storage Features
- **AWS S3/Glacier Support**: Full integration with AWS S3 and Glacier storage classes (Standard, Deep Archive, Intelligent Tiering) with configurable retrieval tiers
- **GCP Cloud Storage (Planned)**: Native support for Google Cloud Storage with Archive and Coldline storage classes
- **Azure Blob Storage (Planned)**: Native support for Azure Blob Storage with Archive tier integration
