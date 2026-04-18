# Portworx Migration Validator

A comprehensive Python tool to validate Portworx storage clusters and assess migration readiness from StoreV1 to StoreV2 in Kubernetes/OpenShift environments.

## Overview

This script connects to your Kubernetes cluster, executes `pxctl` commands on Portworx pods, and performs comprehensive validation checks to ensure your cluster is ready for migration. It analyzes capacity, labels, pool configurations, cloud storage drive types, and generates detailed migration guidance.

## Features

### 🩺 Pod Health Validation
- **Pre-flight Container Readiness Check**: Validates all Portworx pod containers are ready before executing commands
- Retrieves pod status via `kubectl get pods -o json` to check container ready states
- **Prevents false failures**: Avoids cryptic "container not found" errors from unhealthy pods
- **Clear health reporting**: Shows ready vs total pods with detailed status
- **CRITICAL BLOCKER**: Migration cannot proceed if any Portworx pods have unready containers

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

### � Node Disk Capacity Validation
- **Per-node disk inventory**: Uses `pxctl service drive show` to count drives on each node
- **Platform-aware limits**: Checks against maximum drives per node based on cloud provider:
  - **AWS / Azure / GCP / IBM**: 8 drives per node
  - **vSphere**: 12 drives per node  
  - **Pure**: 32 drives per node
- **Available slot calculation**: `Remaining slots = Platform max - Current drives`
- **Pool drive limits**: Default 6 drives per pool (configurable via `limit_drives_per_pool`)
- **Capacity status reporting**:
  - ✅ OK: Sufficient disk slots available
  - ⚠️ LOW: 2 or fewer slots remaining
  - 🚫 AT CAPACITY: No available disk slots
- **WARNING**: Nodes at disk capacity cannot attach additional drives for migration

### 🖥️ Node CPU & Memory Resource Validation
- **StoreV2 Resource Requirements**:
  - **Minimum**: 8 CPU cores, 8 GB RAM per node
  - **Recommended**: 16 CPU cores, 16 GB RAM per node
- Retrieves node capacity and allocatable resources from Kubernetes API
- **Resource status reporting**:
  - ✅ OK: Meets recommended requirements
  - ⚠️ BELOW RECOMMENDED: Meets minimum but not recommended
  - 🚫 BELOW MINIMUM: Does not meet minimum requirements
- **CRITICAL BLOCKER**: Nodes below minimum requirements will block migration
- Provides corrective actions for upgrading node resources

### � License Validation
- **Trial License Blocking**: Parses `pxctl status` output to detect license type
- **CRITICAL BLOCKER**: Trial licenses block StoreV2 migration
- **Volume Attachment Limits**:
  - **Licensed**: 1024 attachments per node
  - **Trial**: 100 attachments per node
- Provides corrective actions to obtain valid license

### 📎 Volume Attachments Per Node
- Retrieves actual volume attachment counts using `kubectl get volumeattachments.storage.k8s.io`
- Compares current attachments against license limits (1024 licensed, 100 trial)
- **Attachment Status Reporting**:
  - ✅ OK: Below 80% of limit
  - ⚠️ HIGH: At 80%+ of limit
  - 🚫 AT LIMIT: At or over attachment limit
- Helps identify nodes that may hit attachment limits during migration
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
    
    # Maximum drives per node by cloud provider
    max_drives_per_node: Dict[str, int] = {
        'aws': 8, 'azure': 8, 'gce': 8, 'gke': 8,
        'google': 8, 'ibm': 8, 'vsphere': 12, 'pure': 32
    }
    
    # Default max drives for unknown providers
    default_max_drives_per_node: int = 8
    
    # Default max drives per pool
    max_drives_per_pool: int = 6
    
    # StoreV2 node resource requirements
    storev2_min_cpu_cores: int = 8
    storev2_min_memory_gb: float = 8.0
    storev2_recommended_cpu_cores: int = 16
    storev2_recommended_memory_gb: float = 16.0
    
    # License requirements for StoreV2 migration
    # Trial licenses are not allowed for migration
    licensed_volume_attachments_per_node: int = 1024
    trial_volume_attachments_per_node: int = 100
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

POD HEALTH:
  Total Portworx Pods: 4
  Pods with All Containers Ready: 4/4

✅ POD HEALTH: All Portworx pods are healthy

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
NODE DISK CAPACITY ANALYSIS
============================================================
Platform: AZURE
Max Drives per Node: 8
Max Drives per Pool: 6

Node                                     Current    Max      Available  Status
---------------------------------------- ---------- -------- ---------- ---------------
worker-node-1                            4          8        4          ✅ OK
worker-node-2                            4          8        4          ✅ OK
worker-node-3                            5          8        3          ✅ OK
worker-node-4                            6          8        2          ⚠️  LOW

📊 Disk Slot Summary:
   ✅ All 4 node(s) have sufficient disk slots available

============================================================
NODE CPU & MEMORY RESOURCE ANALYSIS
============================================================

StoreV2 Requirements:
  Minimum:     8 CPU cores, 8 GB RAM
  Recommended: 16 CPU cores, 16 GB RAM


Node                                CPU      Memory (GB)  Status
----------------------------------- -------- ------------ --------------------
worker-node-1                       16       64.0         ✅ OK
worker-node-2                       16       64.0         ✅ OK
worker-node-3                       8        32.0         ⚠️  BELOW RECOMMENDED
worker-node-4                       16       64.0         ✅ OK

📊 Resource Summary:
   ✅ All 4 node(s) meet minimum resource requirements

============================================================
LICENSE VALIDATION
============================================================

License: PX-Enterprise

✅ LICENSE CHECK: PASSED
   Valid license detected - Migration allowed
   Volume attachment limit: 1024 per node

============================================================
VOLUME ATTACHMENTS PER NODE
============================================================

License Type: Licensed
Attachment Limit per Node: 1024

Node                                     Attached   Limit    Usage    Status
---------------------------------------- ---------- -------- -------- ---------------
worker-node-1                            45         1024     4%       ✅ OK
worker-node-2                            52         1024     5%       ✅ OK
worker-node-3                            38         1024     4%       ✅ OK
worker-node-4                            41         1024     4%       ✅ OK

📊 Attachment Summary:
   ✅ All 4 node(s) have sufficient attachment capacity

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
  License:                  PASSED
  Volume Attachments:       PASSED
  Pod Health:               PASSED
  Pool Health:              PASSED
  Cloud Storage:            PASSED
  Node Disk Capacity:       PASSED
  Node Resources:           PASSED
  Cluster Size:             PASSED
  Metadata Node Labels:     PASSED

✅ MIGRATION READINESS: ALL CHECKS PASSED
   Cluster is ready for StoreV2 migration
```

### Validation Failure Example
```
POD HEALTH:
  Total Portworx Pods: 5
  Pods with All Containers Ready: 3/5

🚫 POD HEALTH: FAILED
   2 pods have unhealthy/unready containers
   Unhealthy pods:
     - px-cluster-abc123-node1 (containers ready: 0/1)
     - px-cluster-abc123-node2 (containers ready: 0/1)

   ✏️  CORRECTIVE ACTION:
   Investigate and fix unhealthy pods before proceeding with migration:
     kubectl -n portworx describe pod px-cluster-abc123-node1
     kubectl -n portworx logs px-cluster-abc123-node1 -c portworx

============================================================
NODE DISK CAPACITY ANALYSIS
============================================================
Platform: AWS
Max Drives per Node: 8
Max Drives per Pool: 6

Node                                     Current    Max      Available  Status
---------------------------------------- ---------- -------- ---------- ---------------
worker-node-1                            8          8        0          🚫 AT CAPACITY
worker-node-2                            7          8        1          ⚠️  LOW
worker-node-3                            4          8        4          ✅ OK

📊 Disk Slot Summary:

🚫 NODES AT DISK CAPACITY (1):
   WARNING: These nodes cannot attach additional drives
   - worker-node-1

   ✏️  CORRECTIVE ACTION:
   For StoreV2 migration, ensure nodes have available disk slots
   Consider removing unused drives or expanding to new nodes

⚠️  NODES NEAR DISK CAPACITY (1):
   These nodes have limited disk slots remaining
   - worker-node-2

============================================================
NODE CPU & MEMORY RESOURCE ANALYSIS
============================================================

StoreV2 Requirements:
  Minimum:     8 CPU cores, 8 GB RAM
  Recommended: 16 CPU cores, 16 GB RAM

Node                                CPU      Memory (GB)  Status
----------------------------------- -------- ------------ --------------------
worker-node-1                       4        4.0          🚫 BELOW MINIMUM
worker-node-2                       8        16.0         ⚠️  BELOW RECOMMENDED
worker-node-3                       16       64.0         ✅ OK

📊 Resource Summary:

🚫 NODES BELOW MINIMUM REQUIREMENTS (1):
   CRITICAL: These nodes do not meet StoreV2 minimum requirements
   - worker-node-1: CPU: 4 < 8, Memory: 4.0 GB < 8 GB

   ✏️  CORRECTIVE ACTION:
   Upgrade nodes to meet minimum: 8 CPU cores, 8 GB RAM
   Or migrate workloads to nodes with sufficient resources

⚠️  NODES BELOW RECOMMENDED RESOURCES (1):
   These nodes meet minimum but not recommended requirements
   - worker-node-2: CPU: 8 < 16
```

============================================================
LICENSE VALIDATION
============================================================

License: Trial (expires in 30 days)

🚫 LICENSE CHECK: FAILED
   CRITICAL: Trial license detected - Migration BLOCKED
   StoreV2 migration requires a valid licensed Portworx installation

   Volume Attachment Limits:
     Trial:    100 attachments per node
     Licensed: 1024 attachments per node

   ✏️  CORRECTIVE ACTION:
   Contact Pure Storage/Portworx support to obtain a valid license
   Apply license using: pxctl license activate <license-key>

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

### Pod Health
- Pre-flight validation of Portworx pod container readiness
- Checks all containers in each pod are in Ready state
- Prevents "container not found" errors during pxctl execution
- CRITICAL blocker if any pods have unready containers

### Data Integrity
- Missing required fields in cluster data
- Unexpected zero values in capacity metrics
- Node and pool reporting validation

### Pool Health
- Offline/unhealthy pool detection (CRITICAL blocker)
- Completely filled pools at 99%+ capacity (CRITICAL blocker)
- Near-full pools at 95%+ capacity (ERROR)

### Node Disk Capacity
- Per-node drive inventory using `pxctl service drive show`
- Platform-specific maximum drive limits (AWS/Azure/GCP: 8, vSphere: 12, Pure: 32)
- Available disk slot calculation for each node
- WARNING if nodes are at or near disk attachment limits
- Ensures sufficient disk slots available for migration

### Node Resources (CPU/Memory)
- Per-node CPU and memory capacity from Kubernetes API
- StoreV2 minimum requirements: 8 CPU cores, 8 GB RAM
- StoreV2 recommended requirements: 16 CPU cores, 16 GB RAM
- CRITICAL blocker if nodes are below minimum requirements
- WARNING if nodes are below recommended requirements

### License
- Parses license type from `pxctl status` output
- Trial licenses block StoreV2 migration (CRITICAL blocker)
- Licensed installations are allowed to proceed

### Volume Attachments
- Retrieves per-node attachment counts from Kubernetes VolumeAttachment objects
- Compares against license limits (1024 for licensed, 100 for trial)
- WARNING if nodes are at 80%+ of attachment limit


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

4. **Pod health validation failures**
   ```bash
   # Check Portworx pod status
   kubectl -n portworx get pods -l name=portworx
   
   # Describe unhealthy pod for details
   kubectl -n portworx describe pod <pod-name>
   
   # Check Portworx container logs
   kubectl -n portworx logs <pod-name> -c portworx
   
   # Common causes:
   # - Portworx container not yet started
   # - Storage initialization in progress
   # - Node connectivity issues
   ```

5. **Nodes at disk capacity**
   ```bash
   # Check drives on a specific node
   kubectl -n portworx exec <pod-name> -- pxctl service drive show
   
   # Check pool and drive mapping
   kubectl -n portworx exec <pod-name> -- pxctl service pool show
   
   # List all block devices on node (OS level)
   kubectl -n portworx exec <pod-name> -- lsblk -d -o NAME,SIZE,TYPE
   
   # Platform limits:
   # - AWS/Azure/GCP/IBM: 8 drives per node
   # - vSphere: 12 drives per node
   # - Pure: 32 drives per node
   
   # Solutions:
   # - Remove unused drives from the node
   # - Add new nodes to the cluster
   # - Redistribute workloads to nodes with available capacity
   ```

6. **Nodes below CPU/memory requirements**
   ```bash
   # Check node resources
   kubectl get nodes -o custom-columns="NAME:.metadata.name,CPU:.status.capacity.cpu,MEMORY:.status.capacity.memory"
   
   # Check allocatable resources (after system reservations)
   kubectl get nodes -o custom-columns="NAME:.metadata.name,CPU:.status.allocatable.cpu,MEMORY:.status.allocatable.memory"
   
   # StoreV2 Requirements:
   # - Minimum: 8 CPU cores, 8 GB RAM
   # - Recommended: 16 CPU cores, 16 GB RAM
   
   # Solutions:
   # - Upgrade node instance type to meet requirements
   # - Add new nodes with sufficient resources
   # - Use larger VM sizes for worker nodes
   # - For on-prem: Add physical CPU/memory to nodes
   ```

7. **Trial license detected**
   ```bash
   # Check current license
   kubectl -n portworx exec <pod-name> -- pxctl status | grep License
   
   # Get detailed license info
   kubectl -n portworx exec <pod-name> -- pxctl license list
   
   # Trial licenses have limited volume attachments (100 per node)
   # StoreV2 migration requires a valid license
   
   # Solutions:
   # - Contact Pure Storage/Portworx support for license
   # - Apply license: pxctl license activate <license-key>
   # - For evaluation: Request extended trial or enterprise license
   ```

8. **Volume attachments at limit**
   ```bash
   # Check volume attachments per node
   kubectl get volumeattachments.storage.k8s.io -o json | \
     jq -r '[.items[] | select(.status.attached==true)] | group_by(.spec.nodeName)[] | "\(.[0].spec.nodeName) \(length)"'
   
   # License limits:
   # - Licensed: 1024 attachments per node
   # - Trial: 100 attachments per node
   
   # Check which PVCs are attached to a node
   kubectl get volumeattachments.storage.k8s.io -o json | \
     jq -r '.items[] | select(.spec.nodeName=="<node-name>") | .spec.source.persistentVolumeName'
   
   # Solutions:
   # - Scale down workloads to reduce attachments
   # - Redistribute pods to other nodes
   # - Upgrade license if on trial
   # - Add more nodes to the cluster
   ```


### Logs
The script generates logs in `stc_validation.log` for debugging purposes.
