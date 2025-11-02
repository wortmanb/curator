# Feature Idea: Multi-Cloud Backend Support for Deepfreeze

## Overview
Implement multi-cloud backend support for Elasticsearch Curator's deepfreeze functionality. This constitutes the next development phase and includes three priorities.

## Priorities

### 1. GCP Cloud Storage Backend Integration
Add support for Google Cloud Platform's Cloud Storage as a backend option for deepfreeze operations.

### 2. Azure Blob Storage Backend Integration
Add support for Microsoft Azure's Blob Storage as a backend option for deepfreeze operations.

### 3. Cloud Provider Selection Framework Enhancement
The --provider option already exists in the setup action but may need tweaking to support the new backends.

## Current Context
- Currently, deepfreeze only supports AWS S3/Glacier via boto3
- The existing code has curator/s3client.py and curator/actions/deepfreeze/
- There's already a --provider option in the setup action that handles provider selection
- The goal is to expand from AWS-only to multi-cloud (AWS, GCP, Azure)

## Spec Information
- **Spec Folder**: `/Users/bret/git/curator/agent-os/specs/2025-11-01-multi-cloud-backends/`
- **Created**: 2025-11-01
