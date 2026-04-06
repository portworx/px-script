# Portworx Migration Validator

A comprehensive Python tool to validate Portworx storage clusters and assess migration readiness from StoreV1 to StoreV2 in Kubernetes/OpenShift environments.

## Overview

This script connects to your Kubernetes cluster, executes `pxctl` commands on Portworx pods, and performs comprehensive validation checks to ensure your cluster is ready for migration. It analyzes capacity, labels, pool configurations, cloud storage drive types, and generates detailed migration guidance.

## Features

### 📊 Portworx Data Retrieval and Analysis
- Automatically discovers Portworx pods via `kubectl -n <namespace> get pods -l name=portworx`
- Executes `pxctl status` to gather cluster-wide capacity and node information
- Executes `pxctl sv pool show` on each node to collect pool configuration and labels
- Retrieves StorageCluster CR spec for cloud storage configuration
- Retrieves Kubernetes node labels for all nodes running Portworx
- Parses and validates storage capacity at cluster, node, and pool levels
- Detects both StoreV1 and StoreV2 nodes (handles `Yes(PX-StoreV2)` format)

### 🎯 Capacity Sizing Recommendations
- Calculates minimum required per-node pool sizes: `total_used / number_of_storage_nodes`
- Provides configurable headroom calculations (default 10%)
- Performs per-node feasibility checks against current capacity
- Identifies undersized nodes that would fail post-migration
- Shows clear comparison: Recommended vs. Available capacity per node
- Enforces cluster-level capacity guardrails with configurable thresholds

### ☁️ Cloud Storage Drive Type Validation
- Retrieves cloud storage configuration from StorageCluster spec
- Validates drive types against supported types for StoreV2 migration
- **Supported drive types by provider:**
  - **AWS**: `gp3`, `io1`
  - **Azure**: `StandardSSD_LRS`, `Premium_LRS`, `PremiumV2_LRS`, `UltraSSD_LRS`
  - **GKE/GCE**: `pd-ssd`
  - **vSphere**: `eagerzeroedthick`, `lazyzeroedthick`, `thin`
- Parses device specs from `cloudStorage.deviceSpecs`, `kvdbDeviceSpec`, and `systemMetadataDeviceSpec`
- **CRITICAL BLOCKER**: Unsupported drive types will block migration

### 🏥 Pool Health & Status Checks
- **Offline Pool Detection**: Identifies pools not in Online/Up/Healthy status
- **Full Pool Detection**: Flags pools at 99%+ capacity as CRITICAL blockers
- **Near-Full Pool Warning**: Warns about pools at 95%+ capacity
- Migration cannot proceed with offline or completely filled pools

### 🔢 Cluster Size Validation
- **Minimum Node Requirement**: StoreV2 migration requires more than 3 nodes
- Validates total Portworx node count (not just storage nodes)
- **CRITICAL BLOCKER**: Clusters with 3 or fewer nodes cannot migrate

### 🏷️ Metadata Node Label Validation (`px/metadata-node`)
- Retrieves labels from Kubernetes nodes where Portworx is running
- Validates `px/metadata-node` label configuration for StoreV2 compatibility
- **Validation Rules:**
  - ✅ **PASS**: No `px/metadata-node` labels present (auto-selection allowed)
  - ✅ **PASS**: 3 nodes have `px/metadata-node=true`, others have no label
  - ❌ **FAIL**: 3 nodes have `px/metadata-node=true` AND other nodes have `px/metadata-node=false`
  - ❌ **FAIL**: All but 3 nodes have `px/metadata-node=false`
- Provides corrective action commands: `kubectl label node <nodename> px/metadata-node-`

### 🏷️ Custom Labels and Metadata Analysis
- **Intelligently filters labels**: Ignores all vendor labels containing `.io/` (kubernetes.io, portworx.io, topology.portworx.io, etc.)
- **Identifies custom labels**: Detects user-defined labels like `iopriority`, `medium` that require manual migration
- **Lists ignored system labels**: Shows all auto-managed labels for transparency
- **Validates label consistency**: Checks for drift across pools
- **Provides migration guidance**: Clear action items for which labels need to be reapplied post-migration

### ⚙️ Pool Priority and IO Configuration
- Inventories IO priorities (HIGH, MEDIUM, LOW, CRITICAL) for each pool
- Validates priorities against StoreV2 allowed values
- Shows priority distribution across all pools
- Generates action items to configure priorities on new storage

### 📋 Migration Readiness Reporting
- **Executive Summary**: Overall status (READY/AT_RISK/BLOCKED), risk level, issue counts
- **Capacity Summary**: Total/used/free capacity with percentages
- **Pool Health Status**: Offline pools, full pools, near-full pools
- **Cloud Storage Summary**: Provider, drive types, supported types validation
- **Metadata Node Labels**: Distribution and validation status
- **Per-Node Analysis**: Capacity comparison, pool counts, node details
- **Custom Labels Section**: Clear separation of custom vs. system labels
- **Action Items**: Prioritized list of pre-migration tasks with specific commands
- **Final Summary**: Pass/Fail/Warning/Skipped counts per validation category
- **Multiple Formats**: Console output, JSON export, detailed text reports

## Installation

### Prerequisites
- Python 3.8+
- kubectl configured with access to your Kubernetes cluster
- Access to Portworx namespace
- PyYAML library

### Setup
```bash
# Install required dependencies
pip3 install pyyaml

# Or install to system Python
python3 -m pip install pyyaml

# Make the script executable
chmod +x migration_validator.py
```

## Usage

### Basic Usage
```bash
# Interactive mode - will prompt for namespace
./migration_validator.py

# Specify namespace directly (recommended)
./migration_validator.py -n portworx

# Generate text report
./migration_validator.py -n portworx -o report.txt

# Verbose logging for troubleshooting
./migration_validator.py -n portworx -v
```

### Command Line Options
- `-n, --namespace`: Portworx namespace (will prompt if not provided)
- `-o, --output`: Output report file (.txt for detailed report)
- `-v, --verbose`: Enable debug logging

### Exit Codes
- `0`: All validations passed - Ready for migration
- `1`: Critical/Error issues found - Migration blocked
- `2`: Warnings present - Proceed with caution

## Configuration

The script uses a default configuration that can be customized:

```python
@dataclass
class STCConfig:
    # Capacity thresholds
    min_free_capacity_percent: float = 20.0
    default_headroom_percent: float = 10.0
    
    # Allowed pool priority values for StoreV2
    allowed_pool_priorities: List[str] = ['high', 'medium', 'low', 'critical']
    
    # Portworx system labels (auto-managed, don't require migration)
    portworx_system_labels: List[str] = ['medium']
    
    # Supported cloud storage drive types per provider
    supported_cloud_drive_types: Dict[str, List[str]] = {
        'aws': ['gp3', 'io1'],
        'azure': ['StandardSSD_LRS', 'Premium_LRS', 'PremiumV2_LRS', 'UltraSSD_LRS'],
        'gce': ['pd-ssd'],
        'gke': ['pd-ssd'],
        'vsphere': ['eagerzeroedthick', 'lazyzeroedthick', 'thin']
    }
```

## Example Output

### Console Summary
```
============================================================
PORTWORX MIGRATION VALIDATION REPORT
============================================================

CLUSTER CAPACITY SUMMARY:
  Total Capacity:  687.5 GB
  Used Capacity:   60.0 GB (8.7%)
  Free Capacity:   627.5 GB (91.3%)

NODE SUMMARY:
  Total Storage Nodes: 4
  Total Storage Pools: 4

============================================================
CLOUD STORAGE DRIVE TYPE VALIDATION
============================================================
  Provider: azure
  Configured Drive Types: Premium_LRS
  Supported Types for AZURE: StandardSSD_LRS, Premium_LRS, PremiumV2_LRS, UltraSSD_LRS

✅ CLOUD STORAGE: All drive types are supported for StoreV2 migration

============================================================
POOL HEALTH & CAPACITY STATUS
============================================================
✅ All pools are online and healthy

============================================================
CLUSTER SIZE & METADATA NODE LABELS
============================================================
  Total Portworx Nodes: 5

✅ CLUSTER SIZE CHECK: PASSED (5 nodes)

Metadata Node Label Distribution (px/metadata-node):
  Total Portworx nodes:  5
  Nodes with 'true':     0
  Nodes with 'false':    0
  Nodes without label:   5

✅ METADATA NODE LABELS: PASSED

============================================================
FINAL VALIDATION SUMMARY
============================================================
  Pool Health:              PASSED
  Cloud Storage:            PASSED
  Cluster Size:             PASSED
  Metadata Node Labels:     PASSED

✅ MIGRATION READINESS: ALL CHECKS PASSED
   Cluster is ready for StoreV2 migration
```

### Validation Failure Example
```
============================================================
CLOUD STORAGE DRIVE TYPE VALIDATION
============================================================
  Provider: azure
  Configured Drive Types: Standard_LRS

🚫 CLOUD STORAGE: BLOCKED
   Unsupported drive type(s): Standard_LRS
   Supported types for AZURE: StandardSSD_LRS, Premium_LRS, PremiumV2_LRS, UltraSSD_LRS

   ✏️  CORRECTIVE ACTION:
   Update cloudStorage.deviceSpecs to use a supported drive type

============================================================
CLUSTER SIZE & METADATA NODE LABELS
============================================================
  Total Portworx Nodes: 3

🚫 CLUSTER SIZE CHECK: FAILED
   Minimum 4 nodes required for StoreV2 migration

Metadata Node Label Distribution (px/metadata-node):
  Total Portworx nodes:  5
  Nodes with 'true':     3
  Nodes with 'false':    2
  Nodes without label:   0

🚫 METADATA NODE LABELS: FAILED
   CRITICAL: 3 nodes have px/metadata-node=true AND 2 nodes have px/metadata-node=false
   This configuration is not supported for StoreV2 migration

   ✏️  CORRECTIVE ACTION:
   Remove the false labels from non-metadata nodes:
     kubectl label node worker-4 px/metadata-node-
     kubectl label node worker-5 px/metadata-node-
```

## Validation Categories

### Data Integrity
- Missing required fields in cluster data
- Unexpected zero values in capacity metrics
- Node and pool reporting validation

### Pool Health
- Offline/unhealthy pool detection (CRITICAL blocker)
- Completely filled pools at 99%+ capacity (CRITICAL blocker)
- Near-full pools at 95%+ capacity (ERROR)

### Cloud Storage
- Drive type validation against supported types per provider
- Supports AWS, Azure, GKE, and vSphere environments
- Parses deviceSpecs, kvdbDeviceSpec, and systemMetadataDeviceSpec

### Cluster Size
- Minimum 4 Portworx nodes required for StoreV2 migration
- Counts all nodes running Portworx (not just storage nodes)

### Metadata Node Labels
- Validates `px/metadata-node` label distribution
- Ensures proper metadata node configuration for StoreV2

### Capacity Planning  
- Per-node sizing feasibility
- Cluster capacity guardrails
- Per-node capacity comparison with recommendations

### Migration Labels
- Required label presence validation
- Cross-node consistency checks

### Pool Configuration
- I/O priority compatibility
- Non-default setting inventory

## Troubleshooting

### Common Issues

1. **kubectl access denied**
   ```bash
   # Verify cluster access
   kubectl cluster-info
   
   # Check Portworx namespace exists
   kubectl get namespaces | grep portworx
   ```

2. **No Portworx pods found**
   ```bash
   # List Portworx pods
   kubectl -n portworx get pods -l name=portworx
   
   # Check if Portworx is running
   kubectl -n portworx get storagecluster
   ```

3. **Permission errors**
   ```bash
   # Verify RBAC permissions
   kubectl auth can-i exec pods -n portworx
   kubectl -n portworx exec <pod-name> -- pxctl status
   ```

### Logs
The script generates logs in `stc_validation.log` for debugging purposes.
