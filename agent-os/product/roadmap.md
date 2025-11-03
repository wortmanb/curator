# Product Roadmap

1. [x] GCP Cloud Storage Backend - Implement GCP Cloud Storage client with support for Archive and Coldline storage classes, including bucket lifecycle management, object restoration, and authentication via service accounts. Complete with end-to-end integration tests against live GCP environment. `L`

2. [ ] Azure Blob Storage Backend - Implement Azure Blob Storage client with support for Archive tier, including container management, blob restoration with rehydration priority tiers, and authentication via managed identities or connection strings. Complete with end-to-end integration tests against live Azure environment. `L`

3. [x] Multi-Cloud Provider Selection - Add provider configuration option to setup command and update CLI to allow users to choose between AWS, GCP, or Azure at initialization time. Validate provider-specific credentials and permissions during setup precondition checks. `M`

4. [ ] Cross-Provider Migration - Build tooling to migrate existing deepfreeze repositories from one cloud provider to another (e.g., AWS to GCP) while preserving snapshot metadata, repository structure, and date ranges. Include validation and rollback capabilities. `XL`

5. [ ] Enhanced Setup Validation - Expand precondition checks to validate cloud provider quotas, billing alerts, storage lifecycle policies, and cost estimation. Provide detailed cost projections based on data volume and retention settings before resources are created. `M`

6. [ ] Automated Cost Reporting - Generate periodic cost reports showing storage expenses by repository, storage tier, and cloud provider. Include cost optimization recommendations based on access patterns and retention policies. `M`

7. [ ] Parallel Thaw Operations - Optimize thaw workflow to restore multiple repositories concurrently using thread pools or async I/O, reducing total time-to-access for multi-repository date ranges from hours to minutes. `M`

8. [ ] Smart Retrieval Tier Selection - Automatically recommend optimal Glacier retrieval tier (Standard/Expedited/Bulk) based on data volume, urgency, and cost constraints. Provide cost-time tradeoff estimates before initiating thaw. `S`

9. [ ] Retention Policy Automation - Implement declarative retention policies that automatically trigger cleanup of repositories exceeding configured age/size thresholds. Include dry-run mode and approval workflows for production safety. `L`

10. [ ] Data Lifecycle Observability - Build monitoring and alerting for deepfreeze operations including setup status, thaw request progress, refreeze completion, and cleanup activity. Export metrics to Prometheus/Grafana for operational visibility. `M`

11. [ ] Repository Health Checks - Add periodic verification that archived repositories remain accessible and uncorrupted. Detect and alert on missing objects, permission issues, or storage tier misconfigurations before they impact restore operations. `M`

12. [ ] Compression Optimization - Evaluate and implement advanced compression strategies for snapshots before archival (LZ4, ZSTD) to reduce storage footprint and transfer costs. Benchmark compression ratios vs. CPU overhead. `S`

> Notes
> - Order items by technical dependencies and product architecture
> - Each item should represent an end-to-end (frontend + backend) functional and testable feature
> - Items 1-3 form the core multi-cloud foundation, items 4-6 enhance the setup/migration experience, items 7-9 optimize operational workflows, and items 10-12 add observability and efficiency improvements
