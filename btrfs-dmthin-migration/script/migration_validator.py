#!/usr/bin/env python3
"""
STC Migration Validator

A comprehensive tool to validate Storage Cluster (STC) data 
and assess migration readiness from StoreV1 to StoreV2.

Author: Operator Team
Date: February 2026
"""

import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import yaml
import logging
from enum import Enum
import argparse
from pathlib import Path


DEFAULT_MAX_PARALLEL = int(os.environ.get('PX_MAX_PARALLEL', '8'))


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('stc_validation.log')
    ]
)
logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Validation severity levels"""
    INFO = "INFO"
    WARNING = "WARNING" 
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ValidationResult:
    """Container for validation results"""
    level: ValidationLevel
    category: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class STCConfig:
    """Configuration for STC validation"""
    # Capacity thresholds
    min_free_capacity_percent: float = 20.0
    default_headroom_percent: float = 10.0
    
    # Allowed pool priority values for StoreV2
    allowed_pool_priorities: List[str] = field(default_factory=lambda: [
        'high', 'medium', 'low', 'critical'
    ])
    
    # Portworx system labels that are auto-managed and don't require migration
    # These labels are populated by Portworx and will be recreated automatically
    portworx_system_labels: List[str] = field(default_factory=lambda: [
        'medium'  # Storage medium type (STORAGE_MEDIUM_SSD, STORAGE_MEDIUM_NVME, etc.)
    ])

    # Supported cloud storage drive types per provider
    # These are the only drive types supported for StoreV2 migration
    supported_cloud_drive_types: Dict[str, List[str]] = field(default_factory=lambda: {
        'aws': ['gp3', 'io1'],
        'azure': ['StandardSSD_LRS', 'Premium_LRS', 'PremiumV2_LRS', 'UltraSSD_LRS'],
        'gce': ['pd-ssd'],  # GKE uses 'gce' as provider name
        'gke': ['pd-ssd'],  # Alternative name
        'google': ['pd-ssd'],  # Another alternative
        'vsphere': ['eagerzeroedthick', 'lazyzeroedthick', 'thin']
    })
    
    # Maximum drives per node by cloud provider
    # StoreV2 migration requires sufficient disk slots for new drives
    max_drives_per_node: Dict[str, int] = field(default_factory=lambda: {
        'aws': 8,
        'azure': 8,
        'gce': 8,
        'gke': 8,
        'google': 8,
        'ibm': 8,
        'vsphere': 12,
        'pure': 32
    })
    
    # Default max drives for unknown providers
    default_max_drives_per_node: int = 8
    
    # Default max drives per pool
    max_drives_per_pool: int = 6
    
    # StoreV2 node resource requirements
    # Minimum: 8 CPU cores, 8 GB RAM
    # Recommended: 16 CPU cores, 16 GB RAM
    storev2_min_cpu_cores: int = 8
    storev2_min_memory_gb: float = 8.0
    storev2_recommended_cpu_cores: int = 16
    storev2_recommended_memory_gb: float = 16.0
    
    # License requirements for StoreV2 migration
    # Trial licenses are not allowed for migration
    # Volume attachment limits per node:
    #   - Licensed: 1024 attachments
    #   - Trial: 100 attachments
    licensed_volume_attachments_per_node: int = 1024
    trial_volume_attachments_per_node: int = 100


class STCDataRetriever:
    """Handles STC data retrieval via kubectl and pxctl"""

    def __init__(self, namespace: str = None, kubeconfig: str = None,
                 max_parallel: int = DEFAULT_MAX_PARALLEL):
        self.namespace = namespace
        self.kubeconfig = kubeconfig
        self.max_parallel = max(1, max_parallel)
        self._auth_token = None
        self._auth_token_checked = False
        self._auth_lock = threading.Lock()
        self._context_lock = threading.Lock()
        self._context_created: set = set()

    def _kubectl_base(self) -> List[str]:
        """Return base kubectl command with optional kubeconfig"""
        cmd = ['kubectl']
        if self.kubeconfig:
            cmd.extend(['--kubeconfig', self.kubeconfig])
        return cmd

    def get_namespace(self) -> str:
        """Get namespace from user if not provided"""
        if not self.namespace:
            self.namespace = input("Please enter the Portworx namespace: ").strip()
            if not self.namespace:
                raise ValueError("Namespace is required to proceed")
        return self.namespace

    def _detect_auth_token(self) -> Optional[str]:
        """Detect if cluster is px-secure and fetch auth token from px-user-token secret.

        Thread-safe: multiple workers may call this concurrently; the lookup runs at most once.
        """
        # Fast path without acquiring the lock
        if self._auth_token_checked:
            return self._auth_token

        with self._auth_lock:
            if self._auth_token_checked:
                return self._auth_token

            namespace = self.get_namespace()

            try:
                import base64
                # Try admin token first (has full access), then fall back to user token
                for secret_name in ['px-admin-token', 'px-user-token']:
                    cmd = self._kubectl_base() + [
                        '-n', namespace, 'get', 'secret', secret_name,
                        '-o', 'jsonpath={.data.auth-token}'
                    ]
                    result = subprocess.run(
                        cmd, capture_output=True, text=True, timeout=15
                    )

                    if result.returncode == 0 and result.stdout.strip():
                        token = base64.b64decode(result.stdout.strip()).decode('utf-8')
                        if token:
                            self._auth_token = token
                            logger.info(f"Detected px-secure cluster — auth token acquired from {secret_name} secret")
                            self._auth_token_checked = True
                            return self._auth_token

                logger.debug("No px-secure token found — cluster is not security-enabled")
                self._auth_token_checked = True
                return None

            except Exception as e:
                logger.debug(f"Auth token detection skipped: {e}")
                self._auth_token_checked = True
                return None

    def _ensure_pxctl_context(self, pod_name: str) -> bool:
        """Create pxctl auth context on the pod for px-secure clusters. Returns True if secure.

        Idempotent per pod: the context is created at most once per (pod, process). Safe under
        concurrent access from multiple worker threads.
        """
        auth_token = self._detect_auth_token()
        if not auth_token:
            return False

        # Fast path — context already created on this pod
        if pod_name in self._context_created:
            return True

        with self._context_lock:
            if pod_name in self._context_created:
                return True

            namespace = self.get_namespace()
            try:
                cmd = self._kubectl_base() + [
                    '-n', namespace, 'exec', pod_name, '-c', 'portworx', '--',
                    '/opt/pwx/bin/pxctl', 'context', 'create', 'admin',
                    f'--token={auth_token}'
                ]
                subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                self._context_created.add(pod_name)
                return True
            except Exception as e:
                logger.debug(f"Failed to create pxctl context on {pod_name}: {e}")
                return False

    def get_portworx_pods(self) -> List[str]:
        """Get list of Portworx pods"""
        namespace = self.get_namespace()

        try:
            cmd = self._kubectl_base() + ['-n', namespace, 'get', 'pods', '-l', 'name=portworx',
                   '-o', 'jsonpath={.items[*].metadata.name}']

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )

            pods = result.stdout.strip().split()
            if not pods:
                raise RuntimeError("No Portworx pods found with label 'name=portworx'")

            logger.info(f"Found {len(pods)} Portworx pods")
            return pods

        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to get Portworx pods: {e.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def get_portworx_pods_with_nodes(self) -> List[Tuple[str, str]]:
        """Get [(pod_name, node_name), ...] for Ready Portworx pods.

        The nodeName lets callers filter to pods running on storage-capable nodes
        before fanning out expensive per-pod pxctl calls, without an extra kubectl round-trip.
        """
        namespace = self.get_namespace()

        try:
            # Use a printable record separator ('|@|') instead of newline — kubectl's
            # jsonpath parser rejects a literal newline delivered by the shell.
            cmd = self._kubectl_base() + [
                '-n', namespace, 'get', 'pods', '-l', 'name=portworx',
                '-o',
                'jsonpath={range .items[*]}{.metadata.name}{","}{.spec.nodeName}{","}'
                '{.status.conditions[?(@.type=="Ready")].status}{"|@|"}{end}'
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )

            pods_with_nodes: List[Tuple[str, str]] = []
            for record in result.stdout.strip().split('|@|'):
                record = record.strip()
                if not record:
                    continue
                parts = record.split(',')
                if len(parts) < 2 or not parts[0] or not parts[1]:
                    continue
                pod_name, node_name = parts[0], parts[1]
                ready = parts[2] if len(parts) >= 3 else ''
                # Skip pods that aren't Ready — kubectl exec against them will hang/fail.
                if ready and ready != 'True':
                    continue
                pods_with_nodes.append((pod_name, node_name))

            return pods_with_nodes

        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to get Portworx pods: {e.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def get_portworx_pod_health(self) -> Dict[str, Any]:
        """Get health status of all Portworx pods including container readiness"""
        namespace = self.get_namespace()

        try:
            # Get detailed pod info in JSON format
            cmd = self._kubectl_base() + ['-n', namespace, 'get', 'pods', '-l', 'name=portworx',
                   '-o', 'json']

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )

            pods_data = json.loads(result.stdout)
            pod_health = {
                'total': 0,
                'ready': [],
                'not_ready': [],
                'details': {}
            }

            for pod in pods_data.get('items', []):
                pod_name = pod.get('metadata', {}).get('name', '')
                pod_phase = pod.get('status', {}).get('phase', 'Unknown')
                
                pod_health['total'] += 1
                
                # Check container statuses for the 'portworx' container
                container_statuses = pod.get('status', {}).get('containerStatuses', [])
                portworx_container_ready = False
                portworx_container_state = 'Not Found'
                
                for container in container_statuses:
                    if container.get('name') == 'portworx':
                        portworx_container_ready = container.get('ready', False)
                        state = container.get('state', {})
                        if 'running' in state:
                            portworx_container_state = 'Running'
                        elif 'waiting' in state:
                            reason = state['waiting'].get('reason', 'Unknown')
                            portworx_container_state = f'Waiting ({reason})'
                        elif 'terminated' in state:
                            reason = state['terminated'].get('reason', 'Unknown')
                            portworx_container_state = f'Terminated ({reason})'
                        break
                
                pod_health['details'][pod_name] = {
                    'phase': pod_phase,
                    'portworx_container_ready': portworx_container_ready,
                    'portworx_container_state': portworx_container_state
                }
                
                # A pod is considered ready if phase is Running AND portworx container is ready
                if pod_phase == 'Running' and portworx_container_ready:
                    pod_health['ready'].append(pod_name)
                else:
                    pod_health['not_ready'].append(pod_name)

            return pod_health

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get Portworx pod health: {e.stderr}")
            return {'total': 0, 'ready': [], 'not_ready': [], 'details': {}}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse pod health JSON: {e}")
            return {'total': 0, 'ready': [], 'not_ready': [], 'details': {}}

    def get_ready_portworx_pods(self) -> Tuple[List[str], Dict[str, Any]]:
        """Get list of ready Portworx pods with health info
        
        Returns:
            Tuple of (list of ready pod names, full health info dict)
        """
        pod_health = self.get_portworx_pod_health()
        
        if pod_health['total'] == 0:
            raise RuntimeError("No Portworx pods found with label 'name=portworx'")
        
        if not pod_health['ready']:
            # Build detailed error message
            not_ready_details = []
            for pod_name in pod_health['not_ready']:
                details = pod_health['details'].get(pod_name, {})
                not_ready_details.append(
                    f"  - {pod_name}: Phase={details.get('phase', 'Unknown')}, "
                    f"Container={details.get('portworx_container_state', 'Unknown')}"
                )
            
            error_msg = (
                f"No healthy Portworx pods available. "
                f"All {pod_health['total']} pod(s) have containers not ready:\n" +
                "\n".join(not_ready_details)
            )
            raise RuntimeError(error_msg)
        
        logger.info(f"Found {len(pod_health['ready'])} ready Portworx pods out of {pod_health['total']} total")
        
        if pod_health['not_ready']:
            logger.warning(
                f"{len(pod_health['not_ready'])} Portworx pod(s) are not ready: "
                f"{', '.join(pod_health['not_ready'][:3])}{'...' if len(pod_health['not_ready']) > 3 else ''}"
            )
        
        return pod_health['ready'], pod_health

    def exec_pxctl_command(self, pod_name: str, command: List[str]) -> str:
        """Execute pxctl command on a Portworx pod (with auth context for px-secure clusters)"""
        namespace = self.get_namespace()

        # Ensure pxctl auth context exists on the pod
        is_secure = self._ensure_pxctl_context(pod_name)

        try:
            logger.debug(f"Executing pxctl {' '.join(command)} on pod: {pod_name}")

            pxctl_cmd = ['/opt/pwx/bin/pxctl']
            if is_secure:
                pxctl_cmd.extend(['--context', 'admin'])
            pxctl_cmd.extend(command)

            cmd = self._kubectl_base() + [
                '-n', namespace, 'exec', pod_name, '-c', 'portworx', '--'
            ] + pxctl_cmd

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60
            )

            logger.debug(f"Successfully executed pxctl {' '.join(command)} on {pod_name}")
            return result.stdout

        except subprocess.CalledProcessError as e:
            error_msg = f"pxctl {' '.join(command)} failed: {e.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def parse_pxctl_status(self, pxctl_output: str) -> Dict[str, Any]:
        """Parse pxctl status output into structured data"""
        data = {
            'kind': 'StorageCluster',
            'metadata': {'name': 'portworx'},
            'status': {
                'capacity': {},
                'nodes': {},
                'pools': {},
                'cluster_id': None,
                'cluster_uuid': None,
                'cluster_backend': None  # 'storev1', 'storev2', or 'mixed'
            },
            'license': {
                'type': None,
                'is_trial': False,
                'expiry_info': None
            }
        }

        lines = pxctl_output.split('\n')

        # Parse cluster ID, UUID, and License
        for line in lines:
            if 'Cluster ID:' in line:
                data['status']['cluster_id'] = line.split(':', 1)[1].strip()
            elif 'Cluster UUID:' in line:
                data['status']['cluster_uuid'] = line.split(':', 1)[1].strip()
            elif line.strip().startswith('License:'):
                license_str = line.split(':', 1)[1].strip()
                data['license']['type'] = license_str
                # Check if trial license
                if 'trial' in license_str.lower():
                    data['license']['is_trial'] = True
                    # Extract expiry info if present (e.g., "Trial (expires in 30 days)")
                    if 'expires' in license_str.lower():
                        data['license']['expiry_info'] = license_str

        # Parse global storage pool
        for i, line in enumerate(lines):
            if 'Global Storage Pool' in line:
                for j in range(i + 1, min(i + 10, len(lines))):
                    if 'Total Used' in lines[j]:
                        parts = lines[j].split(':')[1].strip().split()
                        if len(parts) >= 2:
                            data['status']['capacity']['used'] = self._parse_size(f"{parts[0]} {parts[1]}")
                    elif 'Total Capacity' in lines[j]:
                        parts = lines[j].split(':')[1].strip().split()
                        if len(parts) >= 2:
                            data['status']['capacity']['total'] = self._parse_size(f"{parts[0]} {parts[1]}")
                break

        # Calculate free capacity
        if 'total' in data['status']['capacity'] and 'used' in data['status']['capacity']:
            data['status']['capacity']['free'] = (
                data['status']['capacity']['total'] - data['status']['capacity']['used']
            )

        # Parse node information table
        in_cluster_summary = False
        nodes_parsed = 0
        for i, line in enumerate(lines):
            if 'Cluster Summary' in line:
                in_cluster_summary = True
                continue

            if in_cluster_summary and line.strip():
                # Look for lines starting with IP address
                parts = line.split()
                if len(parts) >= 10:
                    # Check if first part looks like an IP
                    ip_parts = parts[0].split('.')
                    if len(ip_parts) == 4 and all(p.isdigit() for p in ip_parts):
                        node_ip = parts[0]
                        node_id = parts[1]
                        node_name = parts[2]
                        # Skip auth column (parts[3])
                        storage_node = parts[4]
                        used_val = parts[5]
                        used_unit = parts[6]
                        cap_val = parts[7]
                        cap_unit = parts[8]
                        status = parts[9]

                        # Check if storage node (handles "Yes", "Yes(PX-StoreV2)", etc.)
                        if storage_node.lower().startswith('yes'):
                            # Detect storage backend per node. The StorageNode
                            # column reads "Yes" for StoreV1 and "Yes(PX-StoreV2)"
                            # for nodes already migrated to StoreV2.
                            node_backend = 'storev2' if 'storev2' in storage_node.lower() else 'storev1'
                            data['status']['nodes'][node_name] = {
                                'id': node_id,
                                'ip': node_ip,
                                'capacity': {
                                    'used': self._parse_size(f"{used_val} {used_unit}"),
                                    'total': self._parse_size(f"{cap_val} {cap_unit}")
                                },
                                'status': status,
                                'backend': node_backend,
                                'labels': {},
                                'annotations': {},
                                'pools': []
                            }
                            nodes_parsed += 1

            if in_cluster_summary and 'Global Storage Pool' in line:
                break

        # Aggregate per-node backend into a cluster-level backend status so the
        # caller can short-circuit when the cluster is already on StoreV2.
        node_backends = {n.get('backend') for n in data['status']['nodes'].values() if n.get('backend')}
        if not node_backends:
            data['status']['cluster_backend'] = None
        elif node_backends == {'storev2'}:
            data['status']['cluster_backend'] = 'storev2'
        elif node_backends == {'storev1'}:
            data['status']['cluster_backend'] = 'storev1'
        else:
            data['status']['cluster_backend'] = 'mixed'

        logger.info(f"Parsed {nodes_parsed} storage nodes from pxctl status")

        return data

    def parse_pxctl_drive_show(self, pxctl_output: str) -> Dict[str, Any]:
        """Parse pxctl service drive show output to get drive information"""
        drive_info = {
            'total_drives': 0,
            'drives': [],
            'pool_drive_counts': {}  # Pool ID -> drive count
        }

        lines = pxctl_output.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Look for drive entries - typically formatted as:
            # /dev/sdb or similar device paths, or table rows with drive info
            # Common format: "Device   Path   Pool ID   ..."
            # Or: "/dev/sdb   150 GiB   0   Online"
            
            # Skip header lines
            if line.startswith('Device') or line.startswith('Path') or line.startswith('-'):
                continue
            
            # Check if line contains a device path (starts with /dev/ or contains drive info)
            if '/dev/' in line or line.startswith('Drive'):
                drive_info['total_drives'] += 1
                
                # Try to extract pool ID from the line
                parts = line.split()
                for i, part in enumerate(parts):
                    if part.isdigit() and i > 0:  # Pool ID is usually a small number
                        pool_id = part
                        if pool_id not in drive_info['pool_drive_counts']:
                            drive_info['pool_drive_counts'][pool_id] = 0
                        drive_info['pool_drive_counts'][pool_id] += 1
                        break
                
                # Store drive path
                for part in parts:
                    if '/dev/' in part:
                        drive_info['drives'].append(part)
                        break

        return drive_info

    def get_node_drive_info(self, ready_pods: List[str], pod_to_node_map: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        """Get drive information for each node using pxctl service drive show.

        Fans out across pods in a bounded thread pool — each worker only runs the
        pxctl exec; results are merged back on the main thread as they complete.

        Args:
            ready_pods: List of ready pod names
            pod_to_node_map: Mapping of pod names to node names

        Returns:
            Dict mapping node names to their drive information
        """
        node_drive_info = {}
        workers = min(self.max_parallel, max(1, len(ready_pods)))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_pod = {
                executor.submit(self.exec_pxctl_command, pod_name, ['service', 'drive', 'show']): pod_name
                for pod_name in ready_pods
            }
            for future in as_completed(future_to_pod):
                pod_name = future_to_pod[future]
                node_name = pod_to_node_map.get(pod_name, pod_name)
                try:
                    drive_output = future.result()
                    drive_info = self.parse_pxctl_drive_show(drive_output)
                    node_drive_info[node_name] = drive_info
                    logger.info(f"Node {node_name}: {drive_info['total_drives']} drive(s) detected")
                except Exception as e:
                    logger.warning(f"Failed to get drive info from pod {pod_name}: {e}")
                    # Store empty info on failure
                    node_drive_info[node_name] = {
                        'total_drives': 0,
                        'drives': [],
                        'pool_drive_counts': {},
                        'error': str(e)
                    }

        return node_drive_info

    def parse_pxctl_pool_show(self, pxctl_output: str, current_node: str) -> Dict[str, Any]:
        """Parse pxctl sv pool show output"""
        pools = {}

        lines = pxctl_output.split('\n')
        current_pool_id = None
        current_pool = {}

        for line in lines:
            line = line.strip()

            if line.startswith('Pool ID:'):
                # Save previous pool if exists
                if current_pool_id is not None and current_pool:
                    pools[f"pool-{current_pool_id}"] = current_pool

                # Start new pool
                current_pool_id = line.split(':')[1].strip()
                current_pool = {
                    'id': current_pool_id,
                    'node': current_node,
                    'driveType': 'SSD',  # Default
                    'labels': {}
                }

            elif line.startswith('IO Priority:') and current_pool:
                current_pool['priority'] = line.split(':', 1)[1].strip().lower()

            elif line.startswith('Labels:') and current_pool:
                labels_str = line.split(':', 1)[1].strip()
                if labels_str:
                    for label in labels_str.split(','):
                        if '=' in label:
                            key, value = label.split('=', 1)
                            current_pool['labels'][key.strip()] = value.strip()

            elif line.startswith('Size:') and current_pool:
                size_parts = line.split(':')[1].strip().split()
                if len(size_parts) >= 2:
                    current_pool['capacity'] = {
                        'total': self._parse_size(f"{size_parts[0]} {size_parts[1]}")
                    }

            elif line.startswith('Status:') and current_pool:
                current_pool['status'] = line.split(':')[1].strip()

            elif line.startswith('Used:') and current_pool:
                used_parts = line.split(':')[1].strip().split()
                if len(used_parts) >= 2:
                    if 'capacity' not in current_pool:
                        current_pool['capacity'] = {}
                    current_pool['capacity']['used'] = self._parse_size(f"{used_parts[0]} {used_parts[1]}")

            elif 'STORAGE_MEDIUM_SSD' in line:
                current_pool['driveType'] = 'SSD'
            elif 'STORAGE_MEDIUM_NVME' in line:
                current_pool['driveType'] = 'NVME'
            elif 'STORAGE_MEDIUM_HDD' in line:
                current_pool['driveType'] = 'HDD'

        # Save last pool
        if current_pool_id is not None and current_pool:
            pools[f"pool-{current_pool_id}"] = current_pool

        return pools

    def _parse_size(self, size_str: str) -> int:
        """Parse size string (e.g., '12 GiB', '381 GiB') to bytes"""
        size_str = size_str.strip()
        parts = size_str.split()

        if len(parts) != 2:
            return 0

        try:
            value = float(parts[0])
            unit = parts[1].upper()

            multipliers = {
                'B': 1,
                'KB': 1024,
                'MB': 1024**2,
                'GB': 1024**3,
                'TB': 1024**4,
                'KIB': 1024,
                'MIB': 1024**2,
                'GIB': 1024**3,
                'TIB': 1024**4
            }

            return int(value * multipliers.get(unit, 1))
        except (ValueError, KeyError):
            logger.warning(f"Failed to parse size: {size_str}")
            return 0

    def retrieve_stc_data(self) -> Dict[str, Any]:
        """Retrieve STC data using kubectl and pxctl"""
        namespace = self.get_namespace()

        try:
            logger.info(f"Retrieving Portworx data from namespace: {namespace}")

            # Get ready Portworx pods (validates health first)
            ready_pods, pod_health = self.get_ready_portworx_pods()

            # Track nodes/pods that could not be queried so migration checks
            # don't silently treat "no data" as "no issue"
            query_errors = []

            # Execute pxctl status on an available ready pod to get cluster-wide data.
            # Try every ready pod (not just the first) since an individual pod's node
            # can be unreachable (e.g. kubelet down) even though the pod is "ready".
            logger.info("Collecting cluster-wide status...")
            pxctl_status_output = None
            status_pod = None
            for pod in ready_pods:
                try:
                    pxctl_status_output = self.exec_pxctl_command(pod, ['status'])
                    status_pod = pod
                    break
                except Exception as e:
                    logger.warning(f"Failed to get pxctl status from pod {pod}: {e}")
                    query_errors.append({
                        'node': pod,
                        'pod': pod,
                        'stage': 'pxctl status',
                        'error': str(e)
                    })

            if pxctl_status_output is None:
                raise RuntimeError(
                    f"Failed to get pxctl status from any of {len(ready_pods)} ready pod(s)"
                )

            stc_data = self.parse_pxctl_status(pxctl_status_output)

            # Store pod health info in stc_data for reporting
            stc_data['pod_health'] = pod_health

            # Build pod-to-node mapping. get_portworx_pods_with_nodes() already resolves
            # each pod's k8s node name (pod name and pxctl node name can be completely
            # different), so this reuses that single kubectl call instead of a second
            # round trip.
            pod_to_node_map = {}
            try:
                pods_with_nodes = self.get_portworx_pods_with_nodes()
                pod_to_node_map = {pod: node for pod, node in pods_with_nodes if pod in ready_pods}
            except Exception as e:
                logger.warning(f"Failed to build pod-to-node map via kubectl: {e}, falling back to name matching")

            # Fallback: match any ready pod missing from the map by name containment
            for node_name in stc_data['status']['nodes'].keys():
                for pod in ready_pods:
                    if pod not in pod_to_node_map and (node_name in pod or pod in node_name):
                        pod_to_node_map[pod] = node_name
                        break

            # Resolve node names for any pxctl status failures recorded above,
            # now that the pod-to-node map is available
            for err in query_errors:
                err['node'] = pod_to_node_map.get(err['pod'], err['node'])

            # Filter to pods running on storage-capable nodes. Storageless nodes have no
            # pools/drives; querying them just wastes kubectl-exec RTTs. Match on both node
            # name and node IP because pxctl status may report either depending on the
            # cluster's label conventions.
            storage_node_names = set(stc_data['status']['nodes'].keys())
            storage_node_ips = {
                data.get('ip') for data in stc_data['status']['nodes'].values()
                if data.get('ip')
            }
            storage_pods = [
                pod for pod in ready_pods
                if pod_to_node_map.get(pod) in storage_node_names
                or pod_to_node_map.get(pod) in storage_node_ips
            ]

            # Fallback safety: if the filter matched zero pods (e.g. node label scheme
            # doesn't line up), fall back to querying every ready pod. Better to be slow
            # than to skip real storage nodes.
            if storage_pods:
                query_pods = storage_pods
                logger.info(
                    f"Filtered {len(ready_pods)} PX pods → {len(query_pods)} on storage nodes "
                    f"(skipping {len(ready_pods) - len(query_pods)} storageless)"
                )
            else:
                query_pods = ready_pods
                logger.warning(
                    f"Storage-node filter matched 0 pods; falling back to all {len(query_pods)} pods"
                )

            # Warm the auth-token cache once up front so worker threads below don't race on it.
            self._detect_auth_token()

            # Collect pool information from each storage node — fan out across pods in a
            # bounded thread pool since each pxctl exec is an independent kubectl round trip.
            # Every future is resolved back on the main thread, so stc_data/query_errors
            # mutation below needs no lock.
            workers = min(self.max_parallel, max(1, len(query_pods)))
            logger.info(
                f"Collecting pool information from {len(query_pods)} pod(s) "
                f"(parallel, up to {workers} concurrent)..."
            )
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_pod = {
                    executor.submit(self.exec_pxctl_command, pod, ['sv', 'pool', 'show']): pod
                    for pod in query_pods
                }
                completed = 0
                for future in as_completed(future_to_pod):
                    pod = future_to_pod[future]
                    completed += 1
                    try:
                        pool_output = future.result()
                        node_pools = self.parse_pxctl_pool_show(pool_output, pod)
                    except Exception as e:
                        logger.warning(f"Failed to get pool info from pod {pod}: {e}")
                        query_errors.append({
                            'node': pod_to_node_map.get(pod, pod),
                            'pod': pod,
                            'stage': 'sv pool show',
                            'error': str(e)
                        })
                        continue
                    finally:
                        if completed % 25 == 0 or completed == len(query_pods):
                            logger.info(f"  pool-show progress: {completed}/{len(query_pods)}")

                    # Resolve the actual node name for this pod
                    resolved_node = pod_to_node_map.get(pod)

                    # Merge pool data - use pod-scoped key to avoid overwriting
                    # pools with the same ID from different nodes (e.g. pool-0)
                    for pool_name, pool_data in node_pools.items():
                        scoped_pool_name = f"{pod}:{pool_name}"
                        # Set node to the resolved k8s node name
                        if resolved_node:
                            pool_data['node'] = resolved_node
                        stc_data['status']['pools'][scoped_pool_name] = pool_data
                        pool_name = scoped_pool_name

                        # Find matching node and add pool reference
                        matched_node = resolved_node or None
                        if not matched_node:
                            # Fallback: match by IP or name containment
                            for node_name, node_data in stc_data['status']['nodes'].items():
                                if pod in node_name or node_data.get('ip') in pool_data.get('node', ''):
                                    matched_node = node_name
                                    break

                        if matched_node and matched_node in stc_data['status']['nodes']:
                            node_data = stc_data['status']['nodes'][matched_node]
                            pool_data['node'] = matched_node
                            node_data['pools'].append(pool_name)
                            node_data['labels'].update(pool_data.get('labels', {}))

            # Collect drive information from each storage node (also parallelized)
            logger.info("Collecting drive information from each node...")
            node_drive_info = self.get_node_drive_info(query_pods, pod_to_node_map)
            stc_data['node_drive_info'] = node_drive_info
            for node_name, drive_info in node_drive_info.items():
                if drive_info.get('error'):
                    query_errors.append({
                        'node': node_name,
                        'pod': next((p for p, n in pod_to_node_map.items() if n == node_name), node_name),
                        'stage': 'service drive show',
                        'error': drive_info['error']
                    })
            stc_data['query_errors'] = query_errors

            # Collect node CPU and memory resources
            logger.info("Collecting node CPU and memory resources...")
            px_nodes = list(stc_data['status']['nodes'].keys())
            node_resources = self.retrieve_node_resources(px_nodes)
            stc_data['node_resources'] = node_resources

            # Collect volume attachments per node
            logger.info("Collecting volume attachments per node...")
            volume_attachments, k8s_pv_names = self.retrieve_volume_attachments_per_node(px_nodes)
            stc_data['volume_attachments'] = volume_attachments

            # Detect manually attached volumes (attached via pxctl, not via Kubernetes)
            logger.info("Checking for manually attached volumes...")
            px_attached = self.retrieve_px_attached_volumes(status_pod)
            pod_used_pv_names = self.retrieve_pod_used_pv_names()
            manually_attached = {
                vol: node for vol, node in px_attached.items()
                # Not a K8s PV at all — created directly via pxctl
                if vol not in k8s_pv_names
                # K8s PV exists but no running pod is mounting it via a PVC.
                # Kubernetes will not auto-detach this during a node drain.
                or vol not in pod_used_pv_names
            }
            stc_data['manually_attached_volumes'] = manually_attached

            # Retrieve volume HA levels for replication check
            vol_ha = self.retrieve_px_volume_ha_counts(status_pod)
            stc_data['volume_ha_counts'] = vol_ha

            logger.info("Successfully retrieved and parsed Portworx data")
            return stc_data

        except Exception as e:
            error_msg = f"Failed to retrieve STC data: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def validate_kubectl_access(self) -> bool:
        """Validate kubectl access"""
        try:
            # Check kubectl is available
            subprocess.run(self._kubectl_base() + ['version', '--client'],
                         capture_output=True, check=True, timeout=10)

            logger.info("kubectl access validated successfully")
            return True

        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(f"kubectl validation failed: {e}")
            return False

    def retrieve_portworx_nodes(self) -> List[str]:
        """Get list of Kubernetes nodes where Portworx pods are running"""
        namespace = self.get_namespace()

        try:
            logger.info("Retrieving nodes where Portworx is running...")

            # Get Portworx pods with node information
            cmd = self._kubectl_base() + ['-n', namespace, 'get', 'pods', '-l', 'name=portworx',
                   '-o', 'jsonpath={.items[*].spec.nodeName}']

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )

            px_nodes = list(set(result.stdout.strip().split()))  # Remove duplicates
            logger.info(f"Found {len(px_nodes)} nodes running Portworx")
            return px_nodes

        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get Portworx nodes: {e.stderr}")
            return []
        except Exception as e:
            logger.warning(f"Error retrieving Portworx nodes: {e}")
            return []

    def retrieve_k8s_node_labels(self, px_nodes_only: bool = True) -> Dict[str, Dict[str, str]]:
        """Retrieve Kubernetes node labels via kubectl

        Args:
            px_nodes_only: If True, only retrieve labels for nodes running Portworx
        """
        try:
            logger.info("Retrieving Kubernetes node labels...")

            # Get nodes where Portworx is running
            px_nodes = []
            if px_nodes_only:
                px_nodes = self.retrieve_portworx_nodes()

            # Get all nodes with labels in JSON format
            cmd = self._kubectl_base() + ['get', 'nodes', '-o', 'json']

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )

            nodes_data = json.loads(result.stdout)
            node_labels = {}

            for node in nodes_data.get('items', []):
                node_name = node.get('metadata', {}).get('name', '')
                labels = node.get('metadata', {}).get('labels', {})

                if node_name:
                    # If px_nodes_only, filter to only Portworx nodes
                    if px_nodes_only and px_nodes:
                        if node_name in px_nodes:
                            node_labels[node_name] = labels
                    else:
                        node_labels[node_name] = labels

            logger.info(f"Retrieved labels for {len(node_labels)} Kubernetes nodes (Portworx nodes)")
            return node_labels

        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get Kubernetes node labels: {e.stderr}")
            return {}
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse node labels JSON: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Error retrieving Kubernetes node labels: {e}")
            return {}

    def _parse_memory_to_gb(self, mem_str: str) -> float:
        """Parse Kubernetes memory string (e.g., '16384Ki', '16Gi') to GB"""
        if not mem_str:
            return 0.0
        
        mem_str = mem_str.strip()
        
        try:
            if mem_str.endswith('Ki'):
                return float(mem_str[:-2]) / (1024 * 1024)
            elif mem_str.endswith('Mi'):
                return float(mem_str[:-2]) / 1024
            elif mem_str.endswith('Gi'):
                return float(mem_str[:-2])
            elif mem_str.endswith('Ti'):
                return float(mem_str[:-2]) * 1024
            elif mem_str.endswith('K'):
                return float(mem_str[:-1]) / (1000 * 1000)
            elif mem_str.endswith('M'):
                return float(mem_str[:-1]) / 1000
            elif mem_str.endswith('G'):
                return float(mem_str[:-1])
            elif mem_str.endswith('T'):
                return float(mem_str[:-1]) * 1000
            else:
                # Assume bytes
                return float(mem_str) / (1024 * 1024 * 1024)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse memory value: {mem_str}")
            return 0.0

    def _parse_cpu_to_cores(self, cpu_str: str) -> float:
        """Parse Kubernetes CPU string (e.g., '4', '4000m') to cores"""
        if not cpu_str:
            return 0.0
        
        cpu_str = cpu_str.strip()
        
        try:
            if cpu_str.endswith('m'):
                return float(cpu_str[:-1]) / 1000
            else:
                return float(cpu_str)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse CPU value: {cpu_str}")
            return 0.0

    def retrieve_node_resources(self, px_nodes: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Retrieve CPU and memory resources for Kubernetes nodes
        
        Args:
            px_nodes: List of node names to retrieve resources for. If None, retrieves for all Portworx nodes.
            
        Returns:
            Dict mapping node names to their resource info:
            {
                'node-name': {
                    'capacity': {'cpu': 16, 'memory_gb': 64.0},
                    'allocatable': {'cpu': 15.5, 'memory_gb': 62.0}
                }
            }
        """
        try:
            logger.info("Retrieving node CPU and memory resources...")
            
            # Get Portworx nodes if not provided
            if px_nodes is None:
                px_nodes = self.retrieve_portworx_nodes()
            
            if not px_nodes:
                logger.warning("No Portworx nodes found for resource check")
                return {}
            
            # Get all nodes in JSON format
            cmd = self._kubectl_base() + ['get', 'nodes', '-o', 'json']
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )
            
            nodes_data = json.loads(result.stdout)
            node_resources = {}
            
            for node in nodes_data.get('items', []):
                node_name = node.get('metadata', {}).get('name', '')
                
                if node_name not in px_nodes:
                    continue
                
                status = node.get('status', {})
                capacity = status.get('capacity', {})
                allocatable = status.get('allocatable', {})
                
                node_resources[node_name] = {
                    'capacity': {
                        'cpu': self._parse_cpu_to_cores(capacity.get('cpu', '0')),
                        'memory_gb': self._parse_memory_to_gb(capacity.get('memory', '0'))
                    },
                    'allocatable': {
                        'cpu': self._parse_cpu_to_cores(allocatable.get('cpu', '0')),
                        'memory_gb': self._parse_memory_to_gb(allocatable.get('memory', '0'))
                    }
                }
            
            logger.info(f"Retrieved resource info for {len(node_resources)} nodes")
            return node_resources
            
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get node resources: {e.stderr}")
            return {}
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse node resources JSON: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Error retrieving node resources: {e}")
            return {}

    def retrieve_k8s_pv_names(self) -> set:
        """Return the set of all Kubernetes PersistentVolume names.

        Used to distinguish K8s-managed volumes from volumes attached directly via
        pxctl.  Portworx names its PX volumes after the K8s PV (both are
        'pvc-<uuid>'), so a simple name lookup is sufficient.

        This is more reliable than using VolumeAttachment objects because
        in-tree Portworx volumes (kubernetes.io/portworx-volume) do not
        create VolumeAttachment objects even when CSI migration is enabled.
        """
        try:
            cmd = self._kubectl_base() + ['get', 'pv', '-o', 'jsonpath={.items[*].metadata.name}']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60
            )
            names = set(result.stdout.split())
            logger.info(f"Found {len(names)} Kubernetes PersistentVolume(s)")
            return names
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get PersistentVolumes: {e.stderr}")
            return set()
        except Exception as e:
            logger.warning(f"Error retrieving PersistentVolumes: {e}")
            return set()

    def retrieve_pod_used_pv_names(self) -> set:
        """Return the set of PV names actively mounted by running or pending pods.

        A volume that exists as a K8s PV but has no pod currently referencing its
        PVC will not be automatically detached during a node drain — Kubernetes only
        triggers CSI/volume detach when a pod holding the volume is evicted.  Such
        volumes must be detached manually before migration, so we flag them the same
        way as volumes attached outside Kubernetes entirely.
        """
        try:
            # Collect (namespace, pvc_name) pairs referenced by running/pending pods
            cmd = self._kubectl_base() + ['get', 'pods', '-A', '-o', 'json']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60
            )
            pods_data = json.loads(result.stdout)

            pod_pvc_refs: set = set()
            for pod in pods_data.get('items', []):
                phase = pod.get('status', {}).get('phase', '')
                if phase not in ('Running', 'Pending'):
                    continue
                ns = pod['metadata']['namespace']
                for vol in pod.get('spec', {}).get('volumes', []):
                    claim_name = vol.get('persistentVolumeClaim', {}).get('claimName', '')
                    if claim_name:
                        pod_pvc_refs.add((ns, claim_name))

            if not pod_pvc_refs:
                logger.info("No PVCs referenced by running/pending pods")
                return set()

            # Map PVC names to PV names
            cmd2 = self._kubectl_base() + ['get', 'pvc', '-A', '-o', 'json']
            result2 = subprocess.run(
                cmd2,
                capture_output=True,
                text=True,
                check=True,
                timeout=60
            )
            pvcs_data = json.loads(result2.stdout)

            pv_names: set = set()
            for pvc in pvcs_data.get('items', []):
                ns = pvc['metadata']['namespace']
                name = pvc['metadata']['name']
                if (ns, name) in pod_pvc_refs:
                    pv_name = pvc.get('spec', {}).get('volumeName', '')
                    if pv_name:
                        pv_names.add(pv_name)

            logger.info(f"Found {len(pv_names)} PV(s) actively used by running/pending pods")
            return pv_names

        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to retrieve pod PVC references: {e.stderr}")
            return set()
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse pod/PVC JSON: {e}")
            return set()
        except Exception as e:
            logger.warning(f"Error retrieving pod-used PV names: {e}")
            return set()

    def retrieve_volume_attachments_per_node(self, px_nodes: List[str] = None) -> tuple:
        """Retrieve volume attachment counts per node using Kubernetes VolumeAttachment objects.

        Returns:
            Tuple of (node_attachments dict, k8s_pv_names set) where k8s_pv_names is the
            set of all Kubernetes PV names (from kubectl get pv).
        """
        try:
            logger.info("Retrieving volume attachments per node...")

            cmd = self._kubectl_base() + ['get', 'volumeattachments.storage.k8s.io', '-o', 'json']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60
            )

            attachments_data = json.loads(result.stdout)
            node_attachments = {}

            for attachment in attachments_data.get('items', []):
                node_name = attachment.get('spec', {}).get('nodeName', '')

                if not node_name:
                    continue

                # Filter to px_nodes if provided
                if px_nodes and node_name not in px_nodes:
                    continue

                if node_name not in node_attachments:
                    node_attachments[node_name] = {
                        'total_attachments': 0,
                        'attached': 0,
                        'attaching': 0
                    }

                node_attachments[node_name]['total_attachments'] += 1

                status = attachment.get('status', {})
                if status.get('attached', False):
                    node_attachments[node_name]['attached'] += 1
                else:
                    node_attachments[node_name]['attaching'] += 1

            logger.info(f"Retrieved volume attachment info for {len(node_attachments)} nodes")

            # Use PV names (not VolumeAttachment objects) to identify K8s-managed volumes.
            # In-tree Portworx volumes don't create VolumeAttachment objects, so checking
            # only VolumeAttachments produces false positives for every attached volume.
            k8s_pv_names = self.retrieve_k8s_pv_names()
            return node_attachments, k8s_pv_names

        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get volume attachments: {e.stderr}")
            return {}, self.retrieve_k8s_pv_names()
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse volume attachments JSON: {e}")
            return {}, self.retrieve_k8s_pv_names()
        except Exception as e:
            logger.warning(f"Error retrieving volume attachments: {e}")
            return {}, self.retrieve_k8s_pv_names()

    def retrieve_px_attached_volumes(self, pod_name: str) -> Dict[str, str]:
        """Return volumes currently attached in PX via pxctl volume list.

        Returns:
            Dict mapping PX volume name to the node it is attached on.
            Only volumes whose status contains 'attached on' are included.
        """
        import re
        try:
            output = self.exec_pxctl_command(pod_name, ['volume', 'list'])
            attached = {}
            for line in output.splitlines():
                match = re.search(r'\battached\s+on\s+(\S+)', line, re.IGNORECASE)
                if match:
                    parts = line.split()
                    if len(parts) >= 2:
                        vol_name = parts[1]
                        node_name = match.group(1)
                        attached[vol_name] = node_name
            logger.info(f"Found {len(attached)} PX volume(s) in attached state")
            return attached
        except Exception as e:
            logger.warning(f"Failed to retrieve PX attached volumes: {e}")
            return {}

    def retrieve_px_volume_ha_counts(self, pod_name: str) -> Dict[str, int]:
        """Return {volume_name: ha_level} for all PX volumes via pxctl volume list."""
        try:
            output = self.exec_pxctl_command(pod_name, ['volume', 'list'])
            volumes = {}
            for line in output.splitlines():
                parts = line.split('\t')
                if len(parts) >= 4 and parts[0].strip().isdigit():
                    vol_name = parts[1].strip()
                    try:
                        volumes[vol_name] = int(parts[3].strip())
                    except ValueError:
                        pass
            logger.info(f"Found {len(volumes)} PX volume(s) with HA info")
            return volumes
        except Exception as e:
            logger.warning(f"Failed to retrieve PX volume HA counts: {e}")
            return {}

    def retrieve_stc_spec(self) -> Dict[str, Any]:
        """Retrieve StorageCluster spec via kubectl to get cloudStorage configuration"""
        namespace = self.get_namespace()

        try:
            logger.info(f"Retrieving StorageCluster spec from namespace: {namespace}")

            # Get StorageCluster CR
            cmd = self._kubectl_base() + ['-n', namespace, 'get', 'storagecluster', '-o', 'yaml']

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )

            # Parse YAML output
            stc_spec = yaml.safe_load(result.stdout)

            # Handle both single resource and list
            if stc_spec.get('kind') == 'List':
                items = stc_spec.get('items', [])
                if items:
                    stc_spec = items[0]  # Use first StorageCluster
                else:
                    logger.warning("No StorageCluster resources found")
                    return {}

            logger.info("Successfully retrieved StorageCluster spec")
            return stc_spec

        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to get StorageCluster spec: {e.stderr}")
            return {}
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse StorageCluster YAML: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Error retrieving StorageCluster spec: {e}")
            return {}


class STCSanityChecker:
    """Performs sanity checks on STC data"""

    def __init__(self, config: STCConfig):
        self.config = config
        self.results: List[ValidationResult] = []

    def check_missing_fields(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check for missing required fields in STC data"""
        results = []

        if not stc_data:
            results.append(ValidationResult(
                level=ValidationLevel.CRITICAL,
                category="Data Integrity",
                message="No Portworx data retrieved",
                recommendations=["Verify Portworx is installed and running in the namespace"]
            ))
            return results

        # Check pod health first
        pod_health = stc_data.get('pod_health', {})
        if pod_health:
            not_ready = pod_health.get('not_ready', [])
            total = pod_health.get('total', 0)
            ready_count = len(pod_health.get('ready', []))
            
            if not_ready:
                # Build details about unhealthy pods
                unhealthy_details = {}
                for pod_name in not_ready:
                    details = pod_health.get('details', {}).get(pod_name, {})
                    unhealthy_details[pod_name] = {
                        'phase': details.get('phase', 'Unknown'),
                        'container_state': details.get('portworx_container_state', 'Unknown')
                    }
                
                # Determine severity based on how many pods are unhealthy
                if ready_count == 0:
                    level = ValidationLevel.CRITICAL
                    message = f"All {total} Portworx pod(s) are unhealthy - migration cannot proceed"
                elif len(not_ready) > total / 2:
                    level = ValidationLevel.ERROR
                    message = f"Majority of Portworx pods unhealthy: {len(not_ready)} of {total} pods not ready"
                else:
                    level = ValidationLevel.WARNING
                    message = f"Some Portworx pods unhealthy: {len(not_ready)} of {total} pods not ready"
                
                results.append(ValidationResult(
                    level=level,
                    category="Pod Health",
                    message=message,
                    details={"unhealthy_pods": unhealthy_details, "ready_count": ready_count, "total": total},
                    recommendations=[
                        "Ensure all Portworx pods are healthy before migration",
                        "Check pod logs: kubectl logs -n <namespace> <pod-name> -c portworx",
                        "Describe unhealthy pods: kubectl describe pod -n <namespace> <pod-name>",
                        "Verify storage nodes are healthy and accessible"
                    ]
                ))

        # For pxctl-based data, check the structure
        status = stc_data.get('status', {})

        # Check for essential fields
        if not status.get('capacity'):
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                category="Data Integrity",
                message="Cluster capacity data missing",
                recommendations=["Verify Portworx cluster is operational"]
            ))

        if not status.get('nodes'):
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                category="Data Integrity",
                message="No storage nodes found",
                recommendations=["Verify Portworx is running on storage nodes"]
            ))

        if not status.get('pools'):
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                category="Data Integrity",
                message="No storage pools found",
                recommendations=["Verify storage pool configuration"]
            ))

        return results

    def check_zero_values(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check for unexpected zero values in capacity data"""
        results = []

        items = [stc_data] if stc_data else []

        for item in items:
            # Check cluster-level capacity
            capacity = self._get_nested_field(item, 'status.capacity')
            if capacity:
                if capacity.get('total', 0) == 0:
                    results.append(ValidationResult(
                        level=ValidationLevel.ERROR,
                        category="Capacity Validation",
                        message="Total cluster capacity is zero",
                        recommendations=["Check storage provisioning and Portworx installation"]
                    ))

                if capacity.get('used', 0) == 0:
                    results.append(ValidationResult(
                        level=ValidationLevel.INFO,
                        category="Capacity Validation",
                        message="Used capacity is zero - cluster appears empty",
                        details={"capacity": capacity}
                    ))

            # Check node-level capacity
            nodes = self._get_nested_field(item, 'status.nodes')
            if nodes:
                for node_name, node_data in nodes.items():
                    node_capacity = node_data.get('capacity', {})
                    if node_capacity.get('total', 0) == 0:
                        results.append(ValidationResult(
                            level=ValidationLevel.ERROR,
                            category="Node Capacity",
                            message=f"Node {node_name}: Total capacity is zero",
                            details={"node": node_name},
                            recommendations=["Check storage provisioning on this node"]
                        ))

        return results

    def check_missing_pools_nodes(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check for pools/nodes not reporting"""
        results = []

        items = [stc_data] if stc_data else []

        for item in items:
            nodes = self._get_nested_field(item, 'status.nodes') or {}
            pools = self._get_nested_field(item, 'status.pools') or {}

            # Pool assignment checks disabled - not relevant for Portworx migration validation
            # Portworx automatically manages pool-to-node relationships

        return results

    def check_repl3_ha_reduction(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Warn if 3-node cluster has repl-3 volumes that will be temporarily reduced to HA=2 during migration."""
        results = []

        nodes = stc_data.get('status', {}).get('nodes', {})
        if len(nodes) != 3:
            return results

        vol_ha = stc_data.get('volume_ha_counts', {})
        repl3_vols = [name for name, ha in vol_ha.items() if ha == 3]

        if repl3_vols:
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                category="HA Reduction",
                message=f"{len(repl3_vols)} replication-3 volume(s) will be temporarily reduced to HA=2 during migration of each storage node (cluster has only 3 storage nodes)",
                details={"repl3_volume_count": len(repl3_vols), "repl3_volumes": repl3_vols},
                recommendations=[
                    "During migration of each storage node, replication-3 volumes will temporarily run at HA=2",
                    "Ensure no additional node failures occur during this window to avoid data unavailability",
                    "HA=3 will be restored automatically after each node finishes migrating",
                ]
            ))

        return results

    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Safely get nested field from dictionary using dot notation"""
        fields = field_path.split('.')
        current = data

        for field in fields:
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None

        return current


class CapacityAnalyzer:
    """Analyzes capacity and provides sizing recommendations"""

    def __init__(self, config: STCConfig):
        self.config = config

    def calculate_minimum_per_node_size(self, stc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate minimum required per-node pool size"""
        results = {}

        items = [stc_data] if stc_data else []

        for item in items:
            cluster_capacity = self._get_nested_field(item, 'status.capacity')
            nodes = self._get_nested_field(item, 'status.nodes') or {}

            if not cluster_capacity or not nodes:
                continue

            total_used = cluster_capacity.get('used', 0)
            storage_node_count = len([n for n in nodes.values()
                                   if n.get('capacity', {}).get('total', 0) > 0])

            if storage_node_count > 0:
                base_per_node_min = total_used / storage_node_count
                with_headroom = base_per_node_min * (1 + self.config.default_headroom_percent / 100)

                results['cluster'] = {
                    'total_used': total_used,
                    'storage_node_count': storage_node_count,
                    'base_per_node_minimum': base_per_node_min,
                    'with_headroom': with_headroom,
                    'headroom_percent': self.config.default_headroom_percent
                }

        return results

    def check_per_node_feasibility(self, stc_data: Dict[str, Any],
                                 sizing_recommendations: Dict[str, Any]) -> List[ValidationResult]:
        """Check if current nodes can meet sizing recommendations"""
        results = []

        items = [stc_data] if stc_data else []

        for item in items:
            if 'cluster' not in sizing_recommendations:
                continue

            recommended_size = sizing_recommendations['cluster']['with_headroom']
            nodes = self._get_nested_field(item, 'status.nodes') or {}

            undersized_nodes = []

            for node_name, node_data in nodes.items():
                node_capacity = node_data.get('capacity', {}).get('total', 0)
                if node_capacity > 0 and node_capacity < recommended_size:
                    undersized_nodes.append({
                        'node': node_name,
                        'current_capacity': node_capacity,
                        'required_capacity': recommended_size,
                        'deficit': recommended_size - node_capacity
                    })

            if undersized_nodes:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Capacity Planning",
                    message=f"Nodes undersized for migration: {len(undersized_nodes)} nodes",
                    details={"undersized_nodes": undersized_nodes},
                    recommendations=[
                        "Consider expanding storage on undersized nodes",
                        "Or redistribute workloads before migration"
                    ]
                ))

        return results

    def check_cluster_capacity_guardrails(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check cluster-level capacity guardrails"""
        results = []

        items = [stc_data] if stc_data else []

        for item in items:
            capacity = self._get_nested_field(item, 'status.capacity')
            if not capacity:
                continue

            total = capacity.get('total', 0)
            used = capacity.get('used', 0)
            free = capacity.get('free', total - used)

            if total > 0:
                free_percent = (free / total) * 100

                if free_percent < self.config.min_free_capacity_percent:
                    results.append(ValidationResult(
                        level=ValidationLevel.ERROR,
                        category="Capacity Risk",
                        message=f"Low free capacity: {free_percent:.1f}% (threshold: {self.config.min_free_capacity_percent}%)",
                        details={
                            "free_percent": free_percent,
                            "threshold": self.config.min_free_capacity_percent,
                            "capacity": capacity
                        },
                        recommendations=[
                            "Migration may be risky with low free capacity",
                            "Consider expanding storage before migration",
                            "Plan for temporary capacity during migration"
                        ]
                    ))
                elif free_percent < (self.config.min_free_capacity_percent * 1.5):
                    results.append(ValidationResult(
                        level=ValidationLevel.WARNING,
                        category="Capacity Planning",
                        message=f"Moderate free capacity: {free_percent:.1f}%",
                        details={"free_percent": free_percent},
                        recommendations=["Monitor capacity closely during migration"]
                    ))

        return results

    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Safely get nested field from dictionary using dot notation"""
        fields = field_path.split('.')
        current = data

        for field in fields:
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None

        return current


class DriveTypeValidator:
    """Validates drive types and conversion mappings"""

    def __init__(self, config: STCConfig):
        self.config = config

    def detect_drive_types(self, stc_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Detect current drive types used by pools/volumes"""
        drive_types = {'detected': [], 'by_node': {}, 'by_pool': {}}

        items = stc_data.get('items', [stc_data] if stc_data.get('kind') == 'STC' else [])

        for item in items:
            pools = self._get_nested_field(item, 'status.pools') or {}
            nodes = self._get_nested_field(item, 'status.nodes') or {}

            # Extract drive types from pools
            for pool_name, pool_data in pools.items():
                pool_drive_type = pool_data.get('driveType', pool_data.get('storageClass', 'unknown'))
                drive_types['by_pool'][pool_name] = pool_drive_type

                if pool_drive_type not in drive_types['detected']:
                    drive_types['detected'].append(pool_drive_type)

            # Extract drive types from nodes
            for node_name, node_data in nodes.items():
                node_pools = node_data.get('pools', [])
                node_drive_types = []

                for pool_name in node_pools:
                    if pool_name in drive_types['by_pool']:
                        pool_type = drive_types['by_pool'][pool_name]
                        if pool_type not in node_drive_types:
                            node_drive_types.append(pool_type)

                drive_types['by_node'][node_name] = node_drive_types

        return drive_types

    def validate_drive_type_mappings(self, detected_types: List[str]) -> List[ValidationResult]:
        """Validate that all detected drive types have StoreV2 mappings"""
        results = []

        unmapped_types = []
        unsupported_mappings = []

        for drive_type in detected_types:
            if drive_type == 'unknown':
                results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    category="Drive Type Detection",
                    message="Unknown drive type detected",
                    details={"drive_type": drive_type},
                    recommendations=["Verify storage class configuration", "Check STC data collection"]
                ))
                continue

            # Check if mapping exists
            if drive_type not in self.config.drive_type_mappings:
                unmapped_types.append(drive_type)
                continue

            # Check if mapped type is supported in StoreV2
            mapped_type = self.config.drive_type_mappings[drive_type]
            if mapped_type not in self.config.supported_storev2_types:
                unsupported_mappings.append({
                    'source': drive_type,
                    'mapped': mapped_type
                })

        if unmapped_types:
            results.append(ValidationResult(
                level=ValidationLevel.CRITICAL,
                category="Drive Type Mapping",
                message=f"Unmapped drive types found: {', '.join(unmapped_types)}",
                details={"unmapped_types": unmapped_types},
                recommendations=[
                    "Add mappings for these drive types in configuration",
                    "Verify these are valid drive types for your environment"
                ]
            ))

        if unsupported_mappings:
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                category="StoreV2 Compatibility",
                message="Drive types map to unsupported StoreV2 types",
                details={"unsupported_mappings": unsupported_mappings},
                recommendations=[
                    "Update drive type mappings to supported StoreV2 types",
                    "Verify StoreV2 supported drive type list is current"
                ]
            ))

        return results

    def detect_mixed_drive_types(self, drive_types_by_node: Dict[str, List[str]]) -> List[ValidationResult]:
        """Detect nodes/pools with mixed drive types"""
        results = []

        mixed_nodes = {}

        for node_name, node_drive_types in drive_types_by_node.items():
            if len(set(node_drive_types)) > 1:
                mixed_nodes[node_name] = node_drive_types

        if mixed_nodes:
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                category="Drive Type Consistency",
                message=f"Nodes with mixed drive types: {len(mixed_nodes)} nodes",
                details={"mixed_nodes": mixed_nodes},
                recommendations=[
                    "Review migration strategy for mixed drive type nodes",
                    "Consider standardizing drive types before migration",
                    "Plan for potential performance impacts"
                ]
            ))

        return results

    def generate_conversion_plan(self, detected_types: List[str]) -> Dict[str, str]:
        """Generate drive type conversion plan"""
        conversion_plan = {}

        for drive_type in detected_types:
            if drive_type in self.config.drive_type_mappings:
                mapped_type = self.config.drive_type_mappings[drive_type]
                if mapped_type in self.config.supported_storev2_types:
                    conversion_plan[drive_type] = mapped_type

        return conversion_plan

    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Safely get nested field from dictionary using dot notation"""
        fields = field_path.split('.')
        current = data

        for field in fields:
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None

        return current


class MetadataConsistencyChecker:
    """Checks labels and metadata consistency for migration"""

    def __init__(self, config: STCConfig):
        self.config = config

    def check_migration_labels(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check presence of required migration labels"""
        results = []

        items = stc_data.get('items', [stc_data] if stc_data.get('kind') == 'STC' else [])

        for i, item in enumerate(items):
            nodes = self._get_nested_field(item, 'status.nodes') or {}

            nodes_missing_labels = {}

            for node_name, node_data in nodes.items():
                node_labels = node_data.get('labels', {})
                missing_labels = []

                for required_label in self.config.required_migration_labels:
                    if required_label not in node_labels:
                        missing_labels.append(required_label)

                if missing_labels:
                    nodes_missing_labels[node_name] = missing_labels

            if nodes_missing_labels:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Migration Labels",
                    message=f"Nodes missing required migration labels: {len(nodes_missing_labels)} nodes",
                    details={"nodes_missing_labels": nodes_missing_labels},
                    recommendations=[
                        "Add required migration labels to nodes before proceeding",
                        "Use 'kubectl label node <nodename> <label>=<value>' to add labels"
                    ]
                ))

        return results

    def check_metadata_consistency(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check metadata consistency across nodes"""
        results = []

        items = stc_data.get('items', [stc_data] if stc_data.get('kind') == 'STC' else [])

        for i, item in enumerate(items):
            nodes = self._get_nested_field(item, 'status.nodes') or {}

            if len(nodes) < 2:
                continue  # Skip consistency check for single node

            # Collect all custom labels (non-system labels)
            all_custom_labels = set()
            node_labels_map = {}

            for node_name, node_data in nodes.items():
                node_labels = node_data.get('labels', {})
                # Filter out system labels
                custom_labels = {k: v for k, v in node_labels.items()
                               if not k.startswith(('kubernetes.io/', 'node.kubernetes.io/', 'beta.kubernetes.io/'))}

                node_labels_map[node_name] = custom_labels
                all_custom_labels.update(custom_labels.keys())

            # Check for label drift
            label_drift = {}
            for label_key in all_custom_labels:
                values = {}
                for node_name, labels in node_labels_map.items():
                    value = labels.get(label_key, '<missing>')
                    if value not in values:
                        values[value] = []
                    values[value].append(node_name)

                if len(values) > 1:
                    label_drift[label_key] = values

            if label_drift:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Consistency",
                    message=f"Label drift detected across nodes: {len(label_drift)} labels",
                    details={"label_drift": label_drift},
                    recommendations=[
                        "Standardize labels across migration nodes",
                        "Review and update inconsistent label values"
                    ]
                ))

        return results

    def inventory_custom_metadata(self, stc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Inventory custom labels and annotations that need migration"""
        inventory = {
            'pool_labels': {},      # Pool labels with node context
            'node_labels': {},      # Node-level labels
            'annotations': {},
            'pool_label_details': []  # Detailed list of node+pool+label for log file
        }

        items = [stc_data] if stc_data else []

        for item in items:
            # Get labels from POOLS with node context
            pools = self._get_nested_field(item, 'status.pools') or {}

            for pool_name, pool_data in pools.items():
                pool_labels = pool_data.get('labels', {})
                pool_node = pool_data.get('node', 'unknown')

                for label_key, label_value in pool_labels.items():
                    # Track pool labels by label key
                    if label_key not in inventory['pool_labels']:
                        inventory['pool_labels'][label_key] = {}
                    if label_value not in inventory['pool_labels'][label_key]:
                        inventory['pool_labels'][label_key][label_value] = []

                    # Store node+pool combination for uniqueness
                    node_pool_combo = f"{pool_node}:{pool_name}"
                    inventory['pool_labels'][label_key][label_value].append(node_pool_combo)

                    # Add detailed entry for log file
                    inventory['pool_label_details'].append({
                        'node': pool_node,
                        'pool': pool_name,
                        'label_key': label_key,
                        'label_value': label_value
                    })

            # Collect node-level labels separately
            nodes = self._get_nested_field(item, 'status.nodes') or {}

            for node_name, node_data in nodes.items():
                # Inventory node labels
                node_labels = node_data.get('labels', {})

                for label_key, label_value in node_labels.items():
                    if label_key not in inventory['node_labels']:
                        inventory['node_labels'][label_key] = {}
                    if label_value not in inventory['node_labels'][label_key]:
                        inventory['node_labels'][label_key][label_value] = []
                    inventory['node_labels'][label_key][label_value].append(node_name)

                # Inventory node annotations (if any)
                node_annotations = node_data.get('annotations', {})

                for annotation_key, annotation_value in node_annotations.items():
                    if annotation_key not in inventory['annotations']:
                        inventory['annotations'][annotation_key] = {}
                    if annotation_value not in inventory['annotations'][annotation_key]:
                        inventory['annotations'][annotation_key][annotation_value] = []
                    inventory['annotations'][annotation_key][annotation_value].append(node_name)

        return inventory

    def check_metadata_node_labels(self, stc_data: Dict[str, Any], k8s_node_labels: Dict[str, Dict[str, str]] = None, stc_spec: Dict[str, Any] = None) -> List[ValidationResult]:
        """Check px/metadata-node labels for StoreV2 migration requirements

        Args:
            stc_data: Portworx cluster data from pxctl (used for fallback node count)
            k8s_node_labels: Kubernetes node labels from kubectl - these are nodes where Portworx is running
            stc_spec: StorageCluster CR from kubectl (used to check force-ack annotation)
        """
        results = []

        # Check if the user has acknowledged the 3-node KVDB risk via annotation
        force_ack = False
        if stc_spec:
            annotations = stc_spec.get('metadata', {}).get('annotations', {}) or {}
            force_ack = annotations.get('portworx.io/migrate-storev1-to-v2-force-ack', '').lower() == 'true'

        # Use k8s_node_labels as the source of truth for Portworx nodes
        # This includes ALL nodes where Portworx is running, not just storage nodes
        if k8s_node_labels:
            total_nodes = len(k8s_node_labels)
            node_source = "Kubernetes nodes running Portworx"
        else:
            # Fallback to stc_data nodes if k8s labels not available
            items = [stc_data] if stc_data else []
            nodes = {}
            for item in items:
                nodes = self._get_nested_field(item, 'status.nodes') or {}
            total_nodes = len(nodes)
            node_source = "storage nodes from pxctl"

        if total_nodes == 0:
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                category="Metadata Node Labels",
                message="No Portworx nodes found to check metadata labels",
                recommendations=["Verify Portworx is running and kubectl access is available"]
            ))
            return results

        # Rule 4: Warn if cluster has only 3 nodes (KVDB can only run on 3 nodes)
        if total_nodes <= 3:
            if force_ack:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Cluster Size",
                    message=f"KVDB can only run on {total_nodes} node(s) - force-ack annotation found, migration will proceed with KVDB temporarily reduced to 2 members",
                    details={"total_nodes": total_nodes, "node_source": node_source, "force_ack": True},
                    recommendations=[
                        "Monitor KVDB health closely during migration - it will temporarily run with only 2 members",
                        "Ensure KVDB recovers to 3 members after migration completes",
                    ]
                ))
            else:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Cluster Size",
                    message=f"KVDB can only run on {total_nodes} node(s) - migrating a KVDB node will temporarily reduce KVDB members to 2",
                    details={"total_nodes": total_nodes, "node_source": node_source, "force_ack": False},
                    recommendations=[
                        "To migrate this cluster, KVDB members must be temporarily reduced to 2 while a KVDB node is being migrated",
                        "If you accept running with 2 KVDB nodes temporarily, add annotation to StorageCluster: kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true",
                    ]
                ))
            return results

        # Categorize nodes by px/metadata-node label
        nodes_with_true = []
        nodes_with_false = []
        nodes_without_label = []

        if k8s_node_labels:
            # Use Kubernetes node labels (preferred - checks ALL Portworx nodes)
            for node_name, labels in k8s_node_labels.items():
                metadata_label_value = labels.get('px/metadata-node')

                if metadata_label_value is None:
                    nodes_without_label.append(node_name)
                elif str(metadata_label_value).lower() == 'true':
                    nodes_with_true.append(node_name)
                elif str(metadata_label_value).lower() == 'false':
                    nodes_with_false.append(node_name)
        else:
            # Fallback to stc_data labels
            items = [stc_data] if stc_data else []
            for item in items:
                nodes = self._get_nested_field(item, 'status.nodes') or {}
                for node_name, node_data in nodes.items():
                    node_labels = node_data.get('labels', {})
                    metadata_label_value = node_labels.get('px/metadata-node')

                    if metadata_label_value is None:
                        nodes_without_label.append(node_name)
                    elif str(metadata_label_value).lower() == 'true':
                        nodes_with_true.append(node_name)
                    elif str(metadata_label_value).lower() == 'false':
                        nodes_with_false.append(node_name)

        # Rule 1 & 3: Warn if exactly 3 nodes have true AND there are nodes with false
        # KVDB can only run on 3 nodes - migrating a KVDB node requires temporarily running with 2 members.
        if len(nodes_with_true) == 3 and len(nodes_with_false) > 0:
            if force_ack:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Node Labels",
                    message=f"KVDB can only run on 3 metadata nodes (px/metadata-node=true) - force-ack annotation found, migration will proceed with KVDB temporarily reduced to 2 members ({len(nodes_with_false)} node(s) excluded with px/metadata-node=false)",
                    details={"nodes_with_true": nodes_with_true, "nodes_with_false": nodes_with_false, "nodes_without_label": nodes_without_label, "force_ack": True},
                    recommendations=[
                        "Monitor KVDB health closely during migration - it will temporarily run with only 2 members",
                        "Ensure KVDB recovers to 3 members after migration completes",
                    ]
                ))
            else:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Node Labels",
                    message=f"KVDB can only run on 3 metadata nodes (px/metadata-node=true) - migrating a KVDB node will temporarily reduce KVDB members to 2 ({len(nodes_with_false)} node(s) excluded with px/metadata-node=false)",
                    details={"nodes_with_true": nodes_with_true, "nodes_with_false": nodes_with_false, "nodes_without_label": nodes_without_label, "force_ack": False},
                    recommendations=[
                        "To migrate KVDB nodes, KVDB members must be temporarily reduced to 2 during the migration of each KVDB node",
                        "If you accept running with 2 KVDB nodes temporarily, add annotation to StorageCluster: kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true",
                        "Alternatively, add px/metadata-node=true to at least one additional node to reach 4 KVDB nodes:",
                        "Run: kubectl label node <nodename> px/metadata-node=true",
                    ]
                ))

        # Rule: Warn if all but 3 nodes are labeled as px/metadata-node=false
        # KVDB can only run on 3 nodes - requires temporarily running with 2 members.
        elif len(nodes_with_false) == total_nodes - 3:
            if force_ack:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Node Labels",
                    message=f"KVDB can only run on 3 metadata nodes - force-ack annotation found, migration will proceed with KVDB temporarily reduced to 2 members ({len(nodes_with_false)} of {total_nodes} nodes have px/metadata-node=false)",
                    details={"nodes_with_true": nodes_with_true, "nodes_with_false": nodes_with_false, "nodes_without_label": nodes_without_label, "force_ack": True},
                    recommendations=[
                        "Monitor KVDB health closely during migration - it will temporarily run with only 2 members",
                        "Ensure KVDB recovers to 3 members after migration completes",
                    ]
                ))
            else:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Node Labels",
                    message=f"KVDB can only run on 3 metadata nodes - migrating a KVDB node will temporarily reduce KVDB members to 2 ({len(nodes_with_false)} of {total_nodes} nodes have px/metadata-node=false)",
                    details={"nodes_with_true": nodes_with_true, "nodes_with_false": nodes_with_false, "nodes_without_label": nodes_without_label, "force_ack": False},
                    recommendations=[
                        "To migrate KVDB nodes, KVDB members must be temporarily reduced to 2 during the migration of each KVDB node",
                        "If you accept running with 2 KVDB nodes temporarily, add annotation to StorageCluster: kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true",
                        "Alternatively, add px/metadata-node=true to at least one additional node to reach 4 KVDB nodes:",
                        "Run: kubectl label node <nodename> px/metadata-node=true",
                    ]
                ))

        # Rule: Warn if exactly 3 nodes have px/metadata-node=true and the remaining
        # nodes are unlabeled. KVDB will not fail over to unlabeled nodes, so migrating
        # a KVDB node requires temporarily running with 2 members.
        elif len(nodes_with_true) == 3 and len(nodes_with_false) == 0 and len(nodes_without_label) > 0:
            if force_ack:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Node Labels",
                    message=f"KVDB can only run on 3 metadata nodes (px/metadata-node=true) - force-ack annotation found, migration will proceed with KVDB temporarily reduced to 2 members ({len(nodes_without_label)} of {total_nodes} nodes are unlabeled)",
                    details={"nodes_with_true": nodes_with_true, "nodes_with_false": nodes_with_false, "nodes_without_label": nodes_without_label, "force_ack": True},
                    recommendations=[
                        "Monitor KVDB health closely during migration - it will temporarily run with only 2 members",
                        "Ensure KVDB recovers to 3 members after migration completes",
                    ]
                ))
            else:
                results.append(ValidationResult(
                    level=ValidationLevel.WARNING,
                    category="Metadata Node Labels",
                    message=f"KVDB can only run on 3 metadata nodes (px/metadata-node=true) - migrating a KVDB node will temporarily reduce KVDB members to 2 ({len(nodes_without_label)} of {total_nodes} nodes are unlabeled; KVDB will not fail over to unlabeled nodes)",
                    details={"nodes_with_true": nodes_with_true, "nodes_with_false": nodes_with_false, "nodes_without_label": nodes_without_label, "force_ack": False},
                    recommendations=[
                        "To migrate KVDB nodes, KVDB members must be temporarily reduced to 2 during the migration of each KVDB node",
                        "If you accept running with 2 KVDB nodes temporarily, add annotation to StorageCluster: kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true",
                        "Alternatively, add px/metadata-node=true to at least one additional unlabeled node to reach 4 KVDB nodes:",
                        "Run: kubectl label node <nodename> px/metadata-node=true",
                    ]
                ))

        return results

    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Safely get nested field from dictionary using dot notation"""
        fields = field_path.split('.')
        current = data

        for field in fields:
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None

        return current


class PoolConfigurationChecker:
    """Checks pool configuration and I/O settings"""

    def __init__(self, config: STCConfig):
        self.config = config

    def check_pool_priorities(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check pool I/O priority settings"""
        results = []

        items = stc_data.get('items', [stc_data] if stc_data.get('kind') == 'STC' else [])

        invalid_priorities = {}

        for item in items:
            pools = self._get_nested_field(item, 'status.pools') or {}

            for pool_name, pool_data in pools.items():
                priority = pool_data.get('priority', pool_data.get('ioClass', 'medium'))  # default to medium

                if priority not in self.config.allowed_pool_priorities:
                    invalid_priorities[pool_name] = priority

        if invalid_priorities:
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                category="Pool Configuration",
                message=f"Invalid pool priorities detected: {len(invalid_priorities)} pools",
                details={"invalid_priorities": invalid_priorities},
                recommendations=[
                    f"Valid priorities for StoreV2: {', '.join(self.config.allowed_pool_priorities)}",
                    "Update pool priorities before migration"
                ]
            ))

        return results

    def check_pool_status(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check for offline pools and completely filled pools"""
        results = []

        items = [stc_data] if stc_data else []

        offline_pools = {}
        full_pools = {}
        near_full_pools = {}

        # Threshold for "near full" - pools above this % used are flagged
        near_full_threshold = 95.0
        full_threshold = 99.0

        for item in items:
            pools = self._get_nested_field(item, 'status.pools') or {}

            for pool_name, pool_data in pools.items():
                pool_node = pool_data.get('node', 'unknown')
                pool_id = f"{pool_node}:{pool_name}"

                # Check for offline pools
                status = (pool_data.get('status') or '').lower()
                if status and status not in ['online', 'up', 'healthy', '']:
                    offline_pools[pool_id] = {
                        'pool': pool_name,
                        'node': pool_node,
                        'status': pool_data.get('status', 'unknown')
                    }

                # Check for full/near-full pools
                capacity = pool_data.get('capacity', {})
                total = capacity.get('total', 0)
                used = capacity.get('used', 0)

                if total > 0:
                    used_percent = (used / total) * 100

                    if used_percent >= full_threshold:
                        full_pools[pool_id] = {
                            'pool': pool_name,
                            'node': pool_node,
                            'used_percent': round(used_percent, 1),
                            'total_gb': round(total / (1024**3), 2),
                            'used_gb': round(used / (1024**3), 2),
                            'free_gb': round((total - used) / (1024**3), 2)
                        }
                    elif used_percent >= near_full_threshold:
                        near_full_pools[pool_id] = {
                            'pool': pool_name,
                            'node': pool_node,
                            'used_percent': round(used_percent, 1),
                            'total_gb': round(total / (1024**3), 2),
                            'used_gb': round(used / (1024**3), 2),
                            'free_gb': round((total - used) / (1024**3), 2)
                        }

        # Report offline pools - CRITICAL blocker
        if offline_pools:
            results.append(ValidationResult(
                level=ValidationLevel.CRITICAL,
                category="Pool Health",
                message=f"Offline/unhealthy pools detected: {len(offline_pools)} pool(s)",
                details={"offline_pools": offline_pools},
                recommendations=[
                    "Migration cannot proceed with offline pools",
                    "Investigate and resolve pool issues before migration",
                    "Run 'pxctl sv pool show' on affected nodes for details",
                    "Check storage health and connectivity"
                ]
            ))

        # Report completely full pools - CRITICAL blocker
        if full_pools:
            results.append(ValidationResult(
                level=ValidationLevel.CRITICAL,
                category="Pool Capacity",
                message=f"Completely filled pools detected: {len(full_pools)} pool(s) at {full_threshold}%+ capacity",
                details={"full_pools": full_pools},
                recommendations=[
                    "Migration cannot proceed with full pools",
                    "Expand storage capacity or delete unnecessary data",
                    "Full pools cannot accommodate migration overhead",
                    "Consider rebalancing data across nodes"
                ]
            ))

        # Report near-full pools - ERROR (high risk)
        if near_full_pools:
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                category="Pool Capacity",
                message=f"Near-full pools detected: {len(near_full_pools)} pool(s) at {near_full_threshold}%+ capacity",
                details={"near_full_pools": near_full_pools},
                recommendations=[
                    "These pools have very limited free space",
                    "Migration may fail due to insufficient capacity",
                    "Consider expanding storage before proceeding",
                    "Monitor closely during migration"
                ]
            ))

        return results

    def inventory_pool_settings(self, stc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Inventory non-default pool settings"""
        inventory = {'pools': {}}

        items = stc_data.get('items', [stc_data] if stc_data.get('kind') == 'STC' else [])

        # Define default values
        default_settings = {
            'priority': 'medium',
            'replication': 3,
            'compressionEnabled': False,
            'encryptionEnabled': False
        }

        for item in items:
            pools = self._get_nested_field(item, 'status.pools') or {}

            for pool_name, pool_data in pools.items():
                non_default_settings = {}

                # Check each setting against defaults
                for setting_key, default_value in default_settings.items():
                    pool_value = pool_data.get(setting_key)
                    if pool_value is not None and pool_value != default_value:
                        non_default_settings[setting_key] = pool_value

                # Check for other custom settings
                custom_keys = set(pool_data.keys()) - set(default_settings.keys()) - {'name', 'capacity', 'driveType'}
                for key in custom_keys:
                    non_default_settings[key] = pool_data[key]

                if non_default_settings:
                    inventory['pools'][pool_name] = non_default_settings

        return inventory

    def check_pool_distribution(self, stc_data: Dict[str, Any]) -> List[ValidationResult]:
        """Check pool distribution across nodes"""
        results = []

        # Pool distribution and fragmentation checks disabled per user request
        # These checks are not needed for current migration validation

        return results

    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Safely get nested field from dictionary using dot notation"""
        fields = field_path.split('.')
        current = data

        for field in fields:
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None

        return current


class CloudStorageValidator:
    """Validates cloud storage drive types for StoreV2 compatibility"""

    def __init__(self, config: STCConfig):
        self.config = config

    def extract_cloud_storage_info(self, stc_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Extract cloud storage configuration from STC spec"""
        cloud_info = {
            'provider': None,
            'device_specs': [],
            'kvdb_device_spec': None,
            'system_metadata_device_spec': None,
            'drive_types': [],
        }

        if not stc_spec:
            return cloud_info

        # Get cloudStorage section from spec
        spec = stc_spec.get('spec', {})
        cloud_storage = spec.get('cloudStorage', {})

        if not cloud_storage:
            return cloud_info

        # Extract provider
        cloud_info['provider'] = (cloud_storage.get('provider') or '').lower()

        # Extract device specs
        device_specs = cloud_storage.get('deviceSpecs', [])
        cloud_info['device_specs'] = device_specs

        # Extract KVDB device spec
        cloud_info['kvdb_device_spec'] = cloud_storage.get('kvdbDeviceSpec', '')

        # Extract system metadata device spec
        cloud_info['system_metadata_device_spec'] = cloud_storage.get('systemMetadataDeviceSpec', '')

        # Parse drive types from device specs
        for spec_str in device_specs:
            drive_type = self._parse_drive_type(spec_str)
            if drive_type and drive_type not in cloud_info['drive_types']:
                cloud_info['drive_types'].append(drive_type)

        # Parse drive type from KVDB spec
        if cloud_info['kvdb_device_spec']:
            kvdb_type = self._parse_drive_type(cloud_info['kvdb_device_spec'])
            if kvdb_type and kvdb_type not in cloud_info['drive_types']:
                cloud_info['drive_types'].append(kvdb_type)

        # Parse drive type from system metadata device spec
        if cloud_info['system_metadata_device_spec']:
            sys_metadata_type = self._parse_drive_type(cloud_info['system_metadata_device_spec'])
            if sys_metadata_type and sys_metadata_type not in cloud_info['drive_types']:
                cloud_info['drive_types'].append(sys_metadata_type)

        return cloud_info

    def _parse_drive_type(self, spec_str: str) -> Optional[str]:
        """Parse drive type from device spec string like 'type=Standard_LRS,size=400'"""
        if not spec_str:
            return None

        # Handle both formats: "type=Standard_LRS,size=400" and just "Standard_LRS"
        for part in spec_str.split(','):
            part = part.strip()
            if '=' in part:
                key, value = part.split('=', 1)
                if key.strip().lower() == 'type':
                    return value.strip()
            elif part and not part.isdigit():
                # Might be just the type name
                return part

        return None

    def validate_drive_types(self, cloud_info: Dict[str, Any]) -> List[ValidationResult]:
        """Validate cloud storage drive types against supported types"""
        results = []

        provider = cloud_info.get('provider')
        drive_types = cloud_info.get('drive_types', [])
        device_specs = cloud_info.get('device_specs', [])

        if not provider:
            # No cloud storage configured - not an error, might be on-prem
            return results

        if not drive_types:
            # PURE FlashArray deviceSpecs use only size= (no type= parameter) — this
            # is expected and valid; drive type validation does not apply.
            if provider == 'pure' and device_specs:
                return results
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                category="Cloud Storage",
                message="No drive types detected in cloudStorage configuration",
                details={"provider": provider},
                recommendations=["Verify cloudStorage.deviceSpecs is configured correctly"]
            ))
            return results

        # Get supported types for this provider
        supported_types = self.config.supported_cloud_drive_types.get(provider, [])

        if not supported_types:
            # Unknown provider
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                category="Cloud Storage",
                message=f"Unknown cloud provider: {provider}",
                details={"provider": provider, "configured_types": drive_types},
                recommendations=[
                    "Verify the cloud provider name is correct",
                    f"Supported providers: {', '.join(self.config.supported_cloud_drive_types.keys())}"
                ]
            ))
            return results

        # Check each drive type
        unsupported_types = []
        supported_found = []

        for drive_type in drive_types:
            # Case-insensitive check for some providers, exact match for others
            is_supported = False
            for supported in supported_types:
                if provider in ['aws', 'gce', 'gke', 'google']:
                    # Case-insensitive for AWS and GCE
                    if drive_type.lower() == supported.lower():
                        is_supported = True
                        supported_found.append(drive_type)
                        break
                else:
                    # Exact match for Azure (case-sensitive)
                    if drive_type == supported:
                        is_supported = True
                        supported_found.append(drive_type)
                        break

            if not is_supported:
                unsupported_types.append(drive_type)

        if unsupported_types:
            # Format the supported types message per provider
            provider_display = provider.upper()
            if provider in ['gce', 'gke', 'google']:
                provider_display = 'GKE'

            results.append(ValidationResult(
                level=ValidationLevel.CRITICAL,
                category="Cloud Storage",
                message=f"Unsupported drive type(s) detected for {provider_display}: {', '.join(unsupported_types)}",
                details={
                    "provider": provider,
                    "unsupported_types": unsupported_types,
                    "supported_types": supported_types
                },
                recommendations=[
                    f"Migration BLOCKED: Drive type(s) not supported for StoreV2",
                    f"Supported drive types for {provider_display}: {', '.join(supported_types)}",
                    "Update cloudStorage.deviceSpecs to use a supported drive type",
                    "Contact support if you need assistance with drive type migration"
                ]
            ))

        return results

    def get_cloud_storage_summary(self, cloud_info: Dict[str, Any]) -> Dict[str, Any]:
        """Get summary of cloud storage configuration for reporting"""
        summary = {
            'provider': cloud_info.get('provider', 'N/A'),
            'device_specs': cloud_info.get('device_specs', []),
            'kvdb_device_spec': cloud_info.get('kvdb_device_spec', 'N/A'),
            'drive_types': cloud_info.get('drive_types', []),
            'supported_types': []
        }

        provider = cloud_info.get('provider')
        if provider:
            summary['supported_types'] = self.config.supported_cloud_drive_types.get(provider, [])

        return summary


class MigrationReadinessReporter:
    """Generates comprehensive migration readiness reports"""

    def __init__(self, config: STCConfig):
        self.config = config

    def generate_executive_summary(self, all_results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate executive summary of migration readiness"""
        summary = {
            'overall_status': 'READY',
            'risk_level': 'LOW',
            'critical_blockers': 0,
            'warnings': 0,
            'recommendations_count': 0,
            'key_findings': []
        }

        # Count issues by severity
        critical_count = sum(1 for r in all_results if r.level == ValidationLevel.CRITICAL)
        error_count = sum(1 for r in all_results if r.level == ValidationLevel.ERROR)
        warning_count = sum(1 for r in all_results if r.level == ValidationLevel.WARNING)

        summary['critical_blockers'] = critical_count + error_count
        summary['warnings'] = warning_count
        summary['recommendations_count'] = sum(len(r.recommendations) for r in all_results)

        # Determine overall status
        if critical_count > 0:
            summary['overall_status'] = 'BLOCKED'
            summary['risk_level'] = 'CRITICAL'
        elif error_count > 0:
            summary['overall_status'] = 'AT_RISK'
            summary['risk_level'] = 'HIGH'
        elif warning_count > 3:
            summary['overall_status'] = 'CAUTION'
            summary['risk_level'] = 'MEDIUM'

        # Extract key findings
        critical_and_error_results = [r for r in all_results
                                    if r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR]]

        for result in critical_and_error_results[:5]:  # Top 5 most critical
            summary['key_findings'].append({
                'category': result.category,
                'issue': result.message,
                'impact': result.level.value
            })

        return summary

    def generate_detailed_report(self, stc_data: Dict[str, Any],
                               all_results: List[ValidationResult],
                               sizing_recommendations: Dict[str, Any],
                               drive_conversion_plan: Dict[str, str],
                               metadata_inventory: Dict[str, Any],
                               pool_settings_inventory: Dict[str, Any]) -> str:
        """Generate detailed migration readiness report"""

        report_lines = []

        # Header
        report_lines.extend([
            "="*80,
            "STC MIGRATION READINESS ASSESSMENT",
            "="*80,
            f"Generated: 2026-02-20",
            f"Validator Version: 1.0.0",
            ""
        ])

        # Executive Summary
        exec_summary = self.generate_executive_summary(all_results)
        report_lines.extend([
            "EXECUTIVE SUMMARY",
            "-" * 40,
            f"Overall Status: {exec_summary['overall_status']}",
            f"Risk Level: {exec_summary['risk_level']}",
            f"Critical Blockers: {exec_summary['critical_blockers']}",
            f"Warnings: {exec_summary['warnings']}",
            f"Total Recommendations: {exec_summary['recommendations_count']}",
            ""
        ])

        if exec_summary['key_findings']:
            report_lines.extend([
                "Key Findings:",
                ""
            ])
            for i, finding in enumerate(exec_summary['key_findings'], 1):
                report_lines.append(f"  {i}. [{finding['impact']}] {finding['category']}: {finding['issue']}")
            report_lines.append("")

        # Capacity Analysis
        if sizing_recommendations:
            report_lines.extend([
                "CAPACITY ANALYSIS",
                "-" * 40
            ])

            for stc_key, sizing in sizing_recommendations.items():
                report_lines.extend([
                    f"{stc_key.upper()}:",
                    f"  Current Usage: {sizing['total_used']:,.0f} bytes ({sizing['total_used']/(1024**3):.1f} GB)",
                    f"  Storage Nodes: {sizing['storage_node_count']}",
                    f"  Recommended Per-Node: {sizing['with_headroom']:,.0f} bytes ({sizing['with_headroom']/(1024**3):.1f} GB)",
                    f"  Headroom Applied: {sizing['headroom_percent']}%",
                    ""
                ])

        # Drive Type Conversion Plan
        if drive_conversion_plan:
            report_lines.extend([
                "DRIVE TYPE CONVERSION PLAN",
                "-" * 40
            ])

            for source_type, target_type in drive_conversion_plan.items():
                report_lines.append(f"  {source_type} → {target_type}")
            report_lines.append("")

        # Custom Metadata Inventory
        if metadata_inventory.get('pool_labels') or metadata_inventory.get('annotations'):
            report_lines.extend([
                "CUSTOM METADATA REQUIRING MIGRATION",
                "-" * 40
            ])

            if metadata_inventory.get('pool_labels'):
                report_lines.append("Labels:")
                for label_key, values in metadata_inventory.get('pool_labels', {}).items():
                    report_lines.append(f"  {label_key}:")
                    for value, nodes in values.items():
                        report_lines.append(f"    {value}: {', '.join(nodes)}")
                report_lines.append("")

            if metadata_inventory.get('annotations'):
                report_lines.append("Annotations:")
                for annotation_key, values in metadata_inventory['annotations'].items():
                    report_lines.append(f"  {annotation_key}:")
                    for value, nodes in values.items():
                        report_lines.append(f"    {value}: {', '.join(nodes)}")
                report_lines.append("")

        # Non-Default Pool Settings
        if pool_settings_inventory.get('pools'):
            report_lines.extend([
                "NON-DEFAULT POOL SETTINGS",
                "-" * 40
            ])

            for pool_name, settings in pool_settings_inventory['pools'].items():
                report_lines.append(f"  {pool_name}:")
                for setting_key, setting_value in settings.items():
                    report_lines.append(f"    {setting_key}: {setting_value}")
                report_lines.append("")

        # Detailed Validation Results
        report_lines.extend([
            "DETAILED VALIDATION RESULTS",
            "-" * 40
        ])

        # Group by category and level
        by_category = {}
        for result in all_results:
            if result.category not in by_category:
                by_category[result.category] = []
            by_category[result.category].append(result)

        for category, category_results in by_category.items():
            report_lines.extend([
                f"\n{category}:",
                ""
            ])

            for result in category_results:
                report_lines.extend([
                    f"  [{result.level.value}] {result.message}",
                ])

                if result.details:
                    report_lines.append(f"    Details: {json.dumps(result.details, indent=4)}")

                if result.recommendations:
                    report_lines.append("    Recommendations:")
                    for rec in result.recommendations:
                        report_lines.append(f"      - {rec}")

                report_lines.append("")

        # Migration Checklist
        report_lines.extend([
            "PRE-MIGRATION CHECKLIST",
            "-" * 40,
            "□ Review and address all CRITICAL and ERROR level issues",
            "□ Expand storage capacity if below minimum thresholds",
            "□ Apply required migration labels to all nodes",
            "□ Document custom pool settings for reapplication",
            "□ Validate drive type conversion mappings",
            "□ Plan for metadata migration (labels/annotations)",
            "□ Schedule maintenance window for migration",
            "□ Prepare rollback procedures",
            "□ Test migration process in non-production environment",
            ""
        ])

        return "\n".join(report_lines)


def main():
    """Main execution function"""
    # Debug: Script version identifier
    print("=" * 60)
    print("PORTWORX MIGRATION VALIDATOR v2.0 (2026-04-02)")
    print("DEBUG: Running latest script with per-node migration mapping")
    print("=" * 60)

    parser = argparse.ArgumentParser(description="STC Migration Validator")
    parser.add_argument('-n', '--namespace', help='STC namespace')
    parser.add_argument('-k', '--kubeconfig', help='Path to kubeconfig file')
    parser.add_argument('-c', '--config', help='Configuration file path')
    parser.add_argument('-o', '--output', help='Output report file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose logging')
    parser.add_argument(
        '-p', '--max-parallel',
        type=int,
        default=DEFAULT_MAX_PARALLEL,
        help=f'Max concurrent kubectl-exec workers for per-pod pxctl calls '
             f'(default: {DEFAULT_MAX_PARALLEL}; env: PX_MAX_PARALLEL)'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    config = STCConfig()

    try:
        # Initialize components
        retriever = STCDataRetriever(
            namespace=args.namespace,
            kubeconfig=args.kubeconfig,
            max_parallel=args.max_parallel,
        )

        # Validate kubectl access
        if not retriever.validate_kubectl_access():
            logger.error("kubectl access validation failed")
            sys.exit(1)

        # Retrieve STC data
        stc_data = retriever.retrieve_stc_data()

        # Early exit if the cluster is already migrated to StoreV2. Running the
        # pre-migration validator against an already-migrated cluster produces
        # misleading results, so skip all checks and report the state cleanly.
        cluster_backend = stc_data.get('status', {}).get('cluster_backend')
        if cluster_backend == 'storev2':
            nodes = stc_data.get('status', {}).get('nodes', {})
            print(f"\n{'='*70}")
            print(f"{'CLUSTER ALREADY MIGRATED TO STOREV2':^70}")
            print(f"{'='*70}")
            print(f"\n✅ Detected {len(nodes)} node(s) already running on PX-StoreV2 backend.")
            print(f"   Pre-migration validation is not required for this cluster.")
            print(f"   No further checks will be performed.")
            print(f"\n{'='*70}")
            print(f"│ {'FINAL VERDICT: ✅ ALREADY MIGRATED - NO ACTION NEEDED':^66} │")
            print(f"{'─'*70}")
            sys.exit(0)
        elif cluster_backend == 'mixed':
            nodes = stc_data.get('status', {}).get('nodes', {})
            v2_nodes = [n for n, d in nodes.items() if d.get('backend') == 'storev2']
            v1_nodes = [n for n, d in nodes.items() if d.get('backend') == 'storev1']
            print(f"\n{'='*70}")
            print(f"{'CLUSTER MIGRATION IN PROGRESS (MIXED BACKENDS)':^70}")
            print(f"{'='*70}")
            print(f"\n⚠️  Cluster has nodes on both StoreV1 and StoreV2 backends:")
            print(f"   StoreV2 nodes: {len(v2_nodes)}")
            print(f"   StoreV1 nodes: {len(v1_nodes)}")
            print(f"   A migration appears to be in progress or partially complete.")
            print(f"   Re-running pre-migration validation is not appropriate.")
            print(f"\n{'='*70}")
            print(f"│ {'FINAL VERDICT: ⚠️  MIGRATION IN PROGRESS - SKIPPING CHECKS':^66} │")
            print(f"{'─'*70}")
            sys.exit(0)

        # Initialize validators
        sanity_checker = STCSanityChecker(config)
        capacity_analyzer = CapacityAnalyzer(config)
        metadata_checker = MetadataConsistencyChecker(config)
        pool_checker = PoolConfigurationChecker(config)
        reporter = MigrationReadinessReporter(config)

        # Run validations
        all_results = []

        # Nodes/pods that could not be queried (e.g. kubelet unreachable) must
        # block migration rather than being silently skipped - proceeding
        # without this data risks migrating a node in an unknown state.
        query_errors = stc_data.get('query_errors', [])
        if query_errors:
            logger.error(f"{len(query_errors)} node(s)/pod(s) could not be queried during data collection")
            for qe in query_errors:
                all_results.append(ValidationResult(
                    level=ValidationLevel.CRITICAL,
                    category='Node Query Failure',
                    message=(
                        f"Could not inspect node '{qe['node']}' (pod {qe['pod']}) - "
                        f"'{qe['stage']}' failed: {qe['error']}"
                    ),
                    details=qe,
                    recommendations=[
                        f"Check connectivity/health of node '{qe['node']}' and pod '{qe['pod']}'",
                        "Fix the underlying issue (e.g. node unreachable, pod not ready)",
                        "Re-run this validator once the node/pod can be queried successfully"
                    ]
                ))

        # Sanity checks
        logger.info("Running sanity checks...")
        all_results.extend(sanity_checker.check_missing_fields(stc_data))
        all_results.extend(sanity_checker.check_zero_values(stc_data))
        all_results.extend(sanity_checker.check_missing_pools_nodes(stc_data))
        all_results.extend(sanity_checker.check_repl3_ha_reduction(stc_data))

        # Capacity analysis
        logger.info("Analyzing capacity requirements...")
        sizing_recommendations = capacity_analyzer.calculate_minimum_per_node_size(stc_data)
        all_results.extend(capacity_analyzer.check_per_node_feasibility(stc_data, sizing_recommendations))
        all_results.extend(capacity_analyzer.check_cluster_capacity_guardrails(stc_data))

        # Metadata consistency checks
        logger.info("Checking metadata consistency...")
        all_results.extend(metadata_checker.check_metadata_consistency(stc_data))

        # Retrieve Kubernetes node labels and StorageCluster spec
        logger.info("Retrieving Kubernetes node labels...")
        k8s_node_labels = retriever.retrieve_k8s_node_labels()
        logger.info("Retrieving StorageCluster spec...")
        stc_spec = retriever.retrieve_stc_spec()
        all_results.extend(metadata_checker.check_metadata_node_labels(stc_data, k8s_node_labels, stc_spec))
        metadata_inventory = metadata_checker.inventory_custom_metadata(stc_data)

        # Pool configuration checks
        logger.info("Validating pool configurations...")
        all_results.extend(pool_checker.check_pool_priorities(stc_data))
        all_results.extend(pool_checker.check_pool_status(stc_data))
        all_results.extend(pool_checker.check_pool_distribution(stc_data))
        pool_settings_inventory = pool_checker.inventory_pool_settings(stc_data)

        # Cloud storage drive type validation
        logger.info("Validating cloud storage drive types...")
        cloud_storage_validator = CloudStorageValidator(config)
        cloud_storage_info = cloud_storage_validator.extract_cloud_storage_info(stc_spec)
        all_results.extend(cloud_storage_validator.validate_drive_types(cloud_storage_info))

        # Drive type analysis is disabled - set empty values
        drive_conversion_plan = {}
        detected_drive_types = {}

        # Generate comprehensive report
        logger.info("Generating migration readiness report...")

        if args.output and args.output.endswith('.txt'):
            # Generate detailed text report
            detailed_report = reporter.generate_detailed_report(
                stc_data, all_results, sizing_recommendations,
                {}, metadata_inventory, pool_settings_inventory
            )

            with open(args.output, 'w') as f:
                f.write(detailed_report)

            logger.info(f"Detailed report saved to {args.output}")

            # Also display executive summary on console
            exec_summary = reporter.generate_executive_summary(all_results)
            print(f"\nEXECUTIVE SUMMARY:")
            print(f"Status: {exec_summary['overall_status']} (Risk: {exec_summary['risk_level']})")
            print(f"Issues: {exec_summary['critical_blockers']} critical/error, {exec_summary['warnings']} warnings")

        else:
            # Generate console report (original format)
            print("\n" + "="*80)
            print("PORTWORX MIGRATION VALIDATION REPORT")
            print("="*80)

            # Cluster Capacity Summary
            capacity = stc_data.get('status', {}).get('capacity', {})
            nodes = stc_data.get('status', {}).get('nodes', {})
            pools = stc_data.get('status', {}).get('pools', {})

            print(f"\nCLUSTER CAPACITY SUMMARY:")
            if capacity:
                total_gb = capacity.get('total', 0) / (1024**3)
                used_gb = capacity.get('used', 0) / (1024**3)
                free_gb = capacity.get('free', 0) / (1024**3)
                used_pct = (capacity.get('used', 0) / capacity.get('total', 1)) * 100
                free_pct = (capacity.get('free', 0) / capacity.get('total', 1)) * 100

                print(f"  Total Capacity:  {total_gb:,.1f} GB")
                print(f"  Used Capacity:   {used_gb:,.1f} GB ({used_pct:.1f}%)")
                print(f"  Free Capacity:   {free_gb:,.1f} GB ({free_pct:.1f}%)")

            print(f"\nNODE SUMMARY:")
            print(f"  Total PX Nodes (all):       {len(k8s_node_labels) if k8s_node_labels else len(nodes)}")
            print(f"  Total Storage Nodes:        {len(nodes)}")
            print(f"  Total Storageless Nodes:    {(len(k8s_node_labels) - len(nodes)) if k8s_node_labels else 'N/A'}")
            print(f"  Total Storage Pools:        {len(pools)}")

            # Pod health summary
            pod_health = stc_data.get('pod_health', {})
            if pod_health:
                print(f"\nPOD HEALTH SUMMARY:")
                print(f"  Total Portworx Pods:        {pod_health.get('total', 'N/A')}")
                print(f"  Ready Pods:                 {len(pod_health.get('ready', []))}")
                print(f"  Not Ready Pods:             {len(pod_health.get('not_ready', []))}")
                
                if pod_health.get('not_ready'):
                    print(f"\n  ⚠️  UNHEALTHY PODS:")
                    for pod_name in pod_health['not_ready']:
                        details = pod_health.get('details', {}).get(pod_name, {})
                        print(f"    - {pod_name}")
                        print(f"      Phase: {details.get('phase', 'Unknown')}")
                        print(f"      Container: {details.get('portworx_container_state', 'Unknown')}")

            # Sizing calculations
            if sizing_recommendations and 'cluster' in sizing_recommendations:
                sizing = sizing_recommendations['cluster']
                print(f"\nCAPACITY SIZING CALCULATION:")
                print(f"  Total Used:          {sizing['total_used']/(1024**3):,.1f} GB")
                print(f"  Storage Nodes:       {sizing['storage_node_count']}")
                print(f"  Min Per-Node:        {sizing['base_per_node_minimum']/(1024**3):,.1f} GB")
                print(f"  Recommended (+{sizing['headroom_percent']}%): {sizing['with_headroom']/(1024**3):,.1f} GB per node")

                # Calculate average available per node
                if nodes:
                    total_available = sum(node_data.get('capacity', {}).get('total', 0) for node_data in nodes.values())
                    avg_available_per_node = total_available / len(nodes)
                    print(f"\nPER-NODE CAPACITY RECOMMENDATION:")
                    print(f"  Recommended per node: {sizing['with_headroom']/(1024**3):,.1f} GB")
                    print(f"  Available per node:   {avg_available_per_node/(1024**3):,.1f} GB")

                    # Check if nodes meet requirements
                    if avg_available_per_node >= sizing['with_headroom']:
                        deficit_gb = (avg_available_per_node - sizing['with_headroom']) / (1024**3)
                        print(f"  Status: ✅ SUFFICIENT (surplus: {deficit_gb:,.1f} GB per node)")
                    else:
                        deficit_gb = (sizing['with_headroom'] - avg_available_per_node) / (1024**3)
                        print(f"  Status: ⚠️  INSUFFICIENT (deficit: {deficit_gb:,.1f} GB per node)")
                        print(f"           Consider expanding storage before migration")

        # Group results by level for later use in final summary
        by_level = {}
        for result in all_results:
            level = result.level.value
            if level not in by_level:
                by_level[level] = []
            by_level[level].append(result)

        # Print metadata/labels inventory
        if metadata_inventory:
            print(f"\n{'='*60}")
            print(f"CUSTOM LABELS & METADATA ANALYSIS")
            print(f"{'='*60}")

            # =================================================================
            # POOL LABELS ANALYSIS
            # =================================================================
            print(f"\n--- POOL LABELS ---")

            # Separate system labels from custom labels for POOLS
            custom_pool_labels = {}
            system_pool_labels = {}
            portworx_pool_labels = {}

            if metadata_inventory.get('pool_labels'):
                for label_key, values in metadata_inventory['pool_labels'].items():
                    # Check if it's a Portworx managed label (portworx.io/ or topology.portworx.io/)
                    is_px_managed = 'portworx.io/' in label_key

                    # Check if it contains .io/ but NOT portworx.io/ (Kubernetes/other vendor labels)
                    is_k8s_system = '.io/' in label_key and not is_px_managed

                    # Check if it's a Portworx system label (medium, etc.)
                    is_px_system = label_key in config.portworx_system_labels

                    if is_px_managed or is_px_system:
                        portworx_pool_labels[label_key] = values
                    elif is_k8s_system:
                        system_pool_labels[label_key] = values
                    else:
                        custom_pool_labels[label_key] = values

            # Log ignored system labels (Kubernetes .io/ labels - excluding portworx.io/)
            if system_pool_labels:
                print(f"\nℹ️  IGNORED SYSTEM/VENDOR LABELS:")
                print(f"   The following labels are automatically managed and do not require migration:\n")
                for label_key in sorted(system_pool_labels.keys()):
                    print(f"  - {label_key}")

            # Log ignored Portworx managed labels (portworx.io/ and system labels like medium)
            if portworx_pool_labels:
                print(f"\nℹ️  IGNORED PORTWORX MANAGED LABELS:")
                print(f"   The following labels are managed by Portworx and do not require migration:\n")
                for label_key in sorted(portworx_pool_labels.keys()):
                    print(f"  - {label_key}")

            # Print custom pool labels that need migration
            if custom_pool_labels:
                print(f"\n⚠️  CUSTOM POOL LABELS REQUIRING MIGRATION:")
                print(f"   These labels must be manually applied to new storage pools post-migration.")
                print(f"   See ACTION SUMMARY below for full details.")
                print()
                print(f"  📋 ACTION REQUIRED: Reapply these labels after migration")
            else:
                print(f"\n✅ No custom pool labels detected - only system labels present")

            # Check for pool label consistency (include Portworx labels like medium, iopriority)
            all_pool_labels_for_consistency = {**custom_pool_labels, **portworx_pool_labels}
            if all_pool_labels_for_consistency:
                print(f"\nPOOL LABEL CONSISTENCY CHECK:")
                consistent = True
                for label_key, values in all_pool_labels_for_consistency.items():
                    if len(values) > 1:
                        consistent = False
                        print(f"  ⚠️  INCONSISTENT: '{label_key}' has {len(values)} different values")
                        for value, node_pool_list in values.items():
                            print(f"      '{value}': {len(node_pool_list)} pool(s)")
                            # Show the actual node:pool combinations
                            for node_pool in sorted(node_pool_list):
                                print(f"        - {node_pool}")

                if consistent:
                    print(f"  ✅ All pool labels are consistent across pools")

            # =================================================================
            # ANNOTATIONS ANALYSIS
            # =================================================================
            if metadata_inventory.get('annotations'):
                print(f"\n--- ANNOTATIONS ---")
                custom_annotations = {k: v for k, v in metadata_inventory['annotations'].items()
                                    if not any(k.startswith(prefix) for prefix in
                                              ['kubernetes.io/', 'node.kubernetes.io/'])}

                if custom_annotations:
                    print(f"\n⚠️  CUSTOM ANNOTATIONS DETECTED:")
                    for annotation_key, values in custom_annotations.items():
                        print(f"\n  Annotation: {annotation_key}")
                        for value, nodes_list in values.items():
                            print(f"    Value: {value[:50]}{'...' if len(value) > 50 else ''}")
                            print(f"    Nodes: {len(nodes_list)}")
                    print(f"\n  📋 ACTION REQUIRED: Document and reapply these annotations post-migration")
                else:
                    print(f"\n✅ No custom annotations detected")

        # Print license validation
        license_info = stc_data.get('license', {})
        if license_info.get('type'):
            print(f"\n{'='*60}")
            print(f"LICENSE VALIDATION")
            print(f"{'='*60}")
            
            license_type = license_info.get('type', 'Unknown')
            is_trial = license_info.get('is_trial', False)
            
            print(f"\nLicense: {license_type}")
            
            if is_trial:
                print(f"\n🚫 LICENSE CHECK: FAILED")
                print(f"   CRITICAL: Trial license detected - Migration BLOCKED")
                print(f"   StoreV2 migration requires a valid licensed Portworx installation")
                print(f"\n   Volume Attachment Limits:")
                print(f"     Trial:    {config.trial_volume_attachments_per_node} attachments per node")
                print(f"     Licensed: {config.licensed_volume_attachments_per_node} attachments per node")
                print(f"\n   ✏️  CORRECTIVE ACTION:")
                print(f"   Contact Pure Storage/Portworx support to obtain a valid license")
                print(f"   Apply license using: pxctl license activate <license-key>")
            else:
                print(f"\n✅ LICENSE CHECK: PASSED")
                print(f"   Valid license detected - Migration allowed")
                print(f"   Volume attachment limit: {config.licensed_volume_attachments_per_node} per node")

        # Print volume attachments per node
        volume_attachments = stc_data.get('volume_attachments', {})
        license_info = stc_data.get('license', {})
        is_trial = license_info.get('is_trial', False)
        
        if volume_attachments:
            print(f"\n{'='*60}")
            print(f"VOLUME ATTACHMENTS PER NODE")
            print(f"{'='*60}")
            
            # Determine attachment limit based on license
            if is_trial:
                attachment_limit = config.trial_volume_attachments_per_node
                print(f"\nLicense Type: Trial")
            else:
                attachment_limit = config.licensed_volume_attachments_per_node
                print(f"\nLicense Type: Licensed")
            
            print(f"Attachment Limit per Node: {attachment_limit}")
            
            nodes_over_limit = []
            nodes_near_limit = []
            nodes_ok = []
            
            print(f"\n{'Node':<40} {'Attached':<10} {'Limit':<8} {'Usage':<8} {'Status'}")
            print(f"{'-'*40} {'-'*10} {'-'*8} {'-'*8} {'-'*15}")
            
            for node_name, attach_info in volume_attachments.items():
                attached = attach_info.get('attached', 0)
                usage_pct = (attached / attachment_limit) * 100 if attachment_limit > 0 else 0
                
                # Truncate node name for display
                display_name = node_name[:38] + '..' if len(node_name) > 40 else node_name
                
                if attached >= attachment_limit:
                    status = "🚫 AT LIMIT"
                    nodes_over_limit.append({'name': node_name, 'attached': attached})
                elif usage_pct >= 80:
                    status = "⚠️  HIGH"
                    nodes_near_limit.append({'name': node_name, 'attached': attached})
                else:
                    status = "✅ OK"
                    nodes_ok.append(node_name)
                
                print(f"{display_name:<40} {attached:<10} {attachment_limit:<8} {usage_pct:.0f}%{'':<5} {status}")
            
            # Summary
            print(f"\n📊 Attachment Summary:")
            
            if nodes_over_limit:
                print(f"\n🚫 NODES AT/OVER ATTACHMENT LIMIT ({len(nodes_over_limit)}):")
                print(f"   WARNING: These nodes have reached maximum attachments")
                for node in nodes_over_limit[:5]:
                    print(f"   - {node['name']}: {node['attached']} attachments")
                if len(nodes_over_limit) > 5:
                    print(f"   ... and {len(nodes_over_limit) - 5} more")
            
            if nodes_near_limit:
                print(f"\n⚠️  NODES NEAR ATTACHMENT LIMIT ({len(nodes_near_limit)}):")
                print(f"   These nodes are at 80%+ of attachment limit")
                for node in nodes_near_limit[:5]:
                    print(f"   - {node['name']}: {node['attached']} attachments")
                if len(nodes_near_limit) > 5:
                    print(f"   ... and {len(nodes_near_limit) - 5} more")
            
            if not nodes_over_limit and not nodes_near_limit:
                print(f"   ✅ All {len(nodes_ok)} node(s) have sufficient attachment capacity")
            else:
                print(f"\n   Summary:")
                print(f"   Nodes OK:           {len(nodes_ok)}")
                print(f"   Nodes near limit:   {len(nodes_near_limit)}")
                print(f"   Nodes at limit:     {len(nodes_over_limit)}")

        # Print manually attached volumes warning
        manually_attached = stc_data.get('manually_attached_volumes', {})
        if manually_attached:
            print(f"\n{'='*60}")
            print(f"MANUALLY ATTACHED VOLUMES")
            print(f"{'='*60}")
            print(f"\n⚠️  {len(manually_attached)} volume(s) are attached via pxctl (not via Kubernetes).")
            print(f"   These volumes will NOT be detached automatically during migration.")
            print(f"   drain-attachments will time out if they are not detached manually first.\n")
            print(f"   {'Volume':<40} {'Node'}")
            print(f"   {'-'*40} {'-'*40}")
            for vol_name, node_name in manually_attached.items():
                print(f"   {vol_name:<40} {node_name}")
            print(f"\n   ✏️  CORRECTIVE ACTION:")
            print(f"   Detach each volume manually before starting migration:")
            for vol_name in manually_attached:
                print(f"     pxctl volume detach {vol_name}")

        # Print cloud storage drive type validation
        if cloud_storage_info and cloud_storage_info.get('provider'):
            print(f"\n{'='*60}")
            print(f"CLOUD STORAGE DRIVE TYPE VALIDATION")
            print(f"{'='*60}")

            provider = cloud_storage_info.get('provider', '').upper()
            if provider in ['GCE', 'GKE', 'GOOGLE']:
                provider = 'GKE'

            print(f"\nCloud Provider: {provider}")
            print(f"Device Specs: {cloud_storage_info.get('device_specs', [])}")
            if cloud_storage_info.get('kvdb_device_spec'):
                print(f"KVDB Device Spec: {cloud_storage_info.get('kvdb_device_spec')}")
            if cloud_storage_info.get('system_metadata_device_spec'):
                print(f"System Metadata Device Spec: {cloud_storage_info.get('system_metadata_device_spec')}")

            drive_types = cloud_storage_info.get('drive_types', [])
            supported_types = config.supported_cloud_drive_types.get(cloud_storage_info.get('provider', ''), [])

            print(f"\nDetected Drive Types: {drive_types if drive_types else 'None detected'}")
            print(f"Supported Drive Types for {provider}: {', '.join(supported_types) if supported_types else 'Unknown provider'}")

            # Check for unsupported types
            unsupported = []
            for dt in drive_types:
                is_supported = False
                for st in supported_types:
                    if cloud_storage_info.get('provider') in ['aws', 'gce', 'gke', 'google']:
                        if dt.lower() == st.lower():
                            is_supported = True
                            break
                    else:
                        if dt == st:
                            is_supported = True
                            break
                if not is_supported:
                    unsupported.append(dt)

            if unsupported:
                print(f"\n🚫 UNSUPPORTED DRIVE TYPES DETECTED:")
                print(f"   CRITICAL: Migration BLOCKED until resolved")
                for ut in unsupported:
                    print(f"   - {ut}")
                print(f"\n   ✏️  CORRECTIVE ACTION:")
                print(f"   Update cloudStorage.deviceSpecs to use one of the supported types:")
                for st in supported_types:
                    print(f"     - {st}")
            elif drive_types:
                print(f"\n✅ All drive types are supported for StoreV2 migration")
            elif cloud_storage_info.get('provider') == 'pure' and cloud_storage_info.get('device_specs'):
                print(f"\n✅ PURE FlashArray detected - drive type not required in deviceSpecs")
            else:
                print(f"\n⚠️  No drive types detected - verify cloudStorage configuration")

        # Print cluster size and metadata node label analysis
        nodes = stc_data.get('status', {}).get('nodes', {})
        if nodes:
            print(f"\n{'='*60}")
            print(f"CLUSTER SIZE & METADATA NODE VALIDATION")
            print(f"{'='*60}")

            total_nodes = len(nodes)
            print(f"\nTotal Storage Nodes: {total_nodes}")

            # Extract force-ack annotation for display
            force_ack_display = False
            if stc_spec:
                display_annotations = stc_spec.get('metadata', {}).get('annotations', {}) or {}
                force_ack_display = display_annotations.get('portworx.io/migrate-storev1-to-v2-force-ack', '').lower() == 'true'

            # Check minimum node count
            if total_nodes <= 3:
                print(f"\n⚠️  CLUSTER SIZE CHECK: WARNING")
                print(f"   KVDB can only run on {total_nodes} node(s) - migrating a KVDB node will temporarily reduce KVDB members to 2")
                if force_ack_display:
                    print(f"   ✅ Force-ack annotation found: migration will proceed with KVDB temporarily reduced to 2 members")
                    print(f"   Monitor KVDB health closely during migration and ensure it recovers to 3 members afterward")
                else:
                    print(f"\n   ✏️  TO PROCEED:")
                    print(f"   If you accept running with 2 KVDB nodes temporarily during migration, add the annotation:")
                    print(f"     kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true")
            else:
                print(f"\n✅ CLUSTER SIZE CHECK: PASSED ({total_nodes} nodes)")

            # Analyze px/metadata-node labels from Kubernetes node labels (ALL Portworx nodes)
            nodes_with_true = []
            nodes_with_false = []
            nodes_without_label = []

            if k8s_node_labels:
                # Use k8s_node_labels directly - this contains all nodes where Portworx is running
                for node_name, labels in k8s_node_labels.items():
                    metadata_label_value = labels.get('px/metadata-node')

                    if metadata_label_value is None:
                        nodes_without_label.append(node_name)
                    elif str(metadata_label_value).lower() == 'true':
                        nodes_with_true.append(node_name)
                    elif str(metadata_label_value).lower() == 'false':
                        nodes_with_false.append(node_name)

                px_node_count = len(k8s_node_labels)
            else:
                # Fallback to stc_data nodes if k8s labels not available
                for node_name, node_data in nodes.items():
                    nodes_without_label.append(node_name)
                px_node_count = total_nodes

            print(f"\nMetadata Node Label Distribution (px/metadata-node):")
            print(f"  Total Portworx nodes:  {px_node_count}")
            print(f"  Nodes with 'true':     {len(nodes_with_true)}")
            print(f"  Nodes with 'false':    {len(nodes_with_false)}")
            print(f"  Nodes without label:   {len(nodes_without_label)}")

            # Check for 3-node KVDB configurations
            if len(nodes_with_true) == 3 and len(nodes_with_false) > 0:
                print(f"\n⚠️  METADATA NODE LABELS: WARNING")
                print(f"   KVDB can only run on 3 metadata nodes (px/metadata-node=true) - migrating a KVDB node will temporarily reduce KVDB members to 2")
                print(f"   3 nodes have px/metadata-node=true AND {len(nodes_with_false)} nodes have px/metadata-node=false")
                if force_ack_display:
                    print(f"   ✅ Force-ack annotation found: migration will proceed with KVDB temporarily reduced to 2 members")
                    print(f"   Monitor KVDB health closely during migration and ensure it recovers to 3 members afterward")
                else:
                    print(f"\n   ✏️  TO PROCEED:")
                    print(f"   If you accept running with 2 KVDB nodes temporarily during migration, add the annotation:")
                    print(f"     kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true")
                    print(f"   Or add px/metadata-node=true to at least one additional node to reach 4 KVDB nodes:")
                    for node in (nodes_without_label + nodes_with_false):
                        print(f"     kubectl label node {node} px/metadata-node=true")
            elif len(nodes_with_false) == px_node_count - 3:
                print(f"\n⚠️  METADATA NODE LABELS: WARNING")
                print(f"   KVDB can only run on 3 metadata nodes - migrating a KVDB node will temporarily reduce KVDB members to 2")
                print(f"   {len(nodes_with_false)} of {px_node_count} Portworx nodes have px/metadata-node=false")
                if force_ack_display:
                    print(f"   ✅ Force-ack annotation found: migration will proceed with KVDB temporarily reduced to 2 members")
                    print(f"   Monitor KVDB health closely during migration and ensure it recovers to 3 members afterward")
                else:
                    print(f"\n   ✏️  TO PROCEED:")
                    print(f"   If you accept running with 2 KVDB nodes temporarily during migration, add the annotation:")
                    print(f"     kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true")
                    print(f"   Or add px/metadata-node=true to at least one additional node to reach 4 KVDB nodes:")
                    for node in nodes_with_false:
                        print(f"     kubectl label node {node} px/metadata-node=true")
            elif len(nodes_with_true) == 3 and len(nodes_with_false) == 0 and len(nodes_without_label) > 0:
                print(f"\n⚠️  METADATA NODE LABELS: WARNING")
                print(f"   KVDB can only run on 3 metadata nodes (px/metadata-node=true) - migrating a KVDB node will temporarily reduce KVDB members to 2")
                print(f"   3 nodes have px/metadata-node=true AND {len(nodes_without_label)} of {px_node_count} Portworx nodes are unlabeled")
                print(f"   KVDB will not fail over to unlabeled nodes during migration")
                if force_ack_display:
                    print(f"   ✅ Force-ack annotation found: migration will proceed with KVDB temporarily reduced to 2 members")
                    print(f"   Monitor KVDB health closely during migration and ensure it recovers to 3 members afterward")
                else:
                    print(f"\n   ✏️  TO PROCEED:")
                    print(f"   If you accept running with 2 KVDB nodes temporarily during migration, add the annotation:")
                    print(f"     kubectl annotate storagecluster <name> -n <namespace> portworx.io/migrate-storev1-to-v2-force-ack=true")
                    print(f"   Or add px/metadata-node=true to at least one additional unlabeled node to reach 4 KVDB nodes:")
                    for node in nodes_without_label:
                        print(f"     kubectl label node {node} px/metadata-node=true")
            elif len(nodes_with_true) == 3 and len(nodes_with_false) == 0:
                print(f"\n✅ METADATA NODE LABELS: PASSED")
                print(f"   3 metadata nodes designated (acceptable)")
            else:
                print(f"\n✅ METADATA NODE LABELS: PASSED")

            # HA reduction warning for repl-3 volumes on 3-node clusters
            if total_nodes == 3:
                vol_ha = stc_data.get('volume_ha_counts', {})
                repl3_vols = [name for name, ha in vol_ha.items() if ha == 3]
                if repl3_vols:
                    print(f"\n⚠️  VOLUME HA DURING MIGRATION: WARNING")
                    print(f"   {len(repl3_vols)} replication-3 volume(s) will be temporarily reduced to HA=2 during migration of each storage node")
                    print(f"   Ensure no additional node failures occur during this window to avoid data unavailability")
                    print(f"   HA=3 will be restored automatically after each node finishes migrating")
                    if len(repl3_vols) <= 10:
                        print(f"\n   Affected volumes:")
                        for vol in repl3_vols:
                            print(f"     - {vol}")
                else:
                    print(f"\n✅ VOLUME HA DURING MIGRATION: PASSED")
                    print(f"   No replication-3 volumes detected - no HA reduction will occur")

        # Print pool health analysis (offline/full pools)
        pools = stc_data.get('status', {}).get('pools', {})
        if pools:
            print(f"\n{'='*60}")
            print(f"POOL HEALTH & CAPACITY STATUS")
            print(f"{'='*60}")

            offline_pools = []
            full_pools = []
            near_full_pools = []
            healthy_pools = []

            for pool_name, pool_data in pools.items():
                pool_node = pool_data.get('node', 'unknown')
                pool_id = f"{pool_node}:{pool_name}"

                # Check status
                status = (pool_data.get('status') or '').lower()
                is_offline = status and status not in ['online', 'up', 'healthy', '']

                # Check capacity
                capacity = pool_data.get('capacity', {})
                total = capacity.get('total', 0)
                used = capacity.get('used', 0)
                used_percent = (used / total * 100) if total > 0 else 0

                pool_info = {
                    'id': pool_id,
                    'status': pool_data.get('status', 'unknown'),
                    'used_percent': round(used_percent, 1),
                    'total_gb': round(total / (1024**3), 2),
                    'free_gb': round((total - used) / (1024**3), 2)
                }

                if is_offline:
                    offline_pools.append(pool_info)
                elif used_percent >= 99.0:
                    full_pools.append(pool_info)
                elif used_percent >= 95.0:
                    near_full_pools.append(pool_info)
                else:
                    healthy_pools.append(pool_info)

            # Print offline pools (CRITICAL)
            if offline_pools:
                print(f"\n🚫 OFFLINE/UNHEALTHY POOLS ({len(offline_pools)}):")
                print(f"   CRITICAL: Migration BLOCKED until resolved")
                for p in offline_pools:
                    print(f"   - {p['id']}: Status={p['status']}")

            # Print full pools (CRITICAL)
            if full_pools:
                print(f"\n🔴 COMPLETELY FILLED POOLS ({len(full_pools)}):")
                print(f"   CRITICAL: Migration BLOCKED - no capacity for migration overhead")
                for p in full_pools:
                    print(f"   - {p['id']}: {p['used_percent']}% used ({p['free_gb']} GB free)")

            # Print near-full pools (ERROR)
            if near_full_pools:
                print(f"\n🟠 NEAR-FULL POOLS ({len(near_full_pools)}):")
                print(f"   ERROR: High risk of migration failure")
                for p in near_full_pools:
                    print(f"   - {p['id']}: {p['used_percent']}% used ({p['free_gb']} GB free)")

            # Summary
            if not offline_pools and not full_pools and not near_full_pools:
                print(f"\n✅ All {len(healthy_pools)} pool(s) are healthy and have sufficient capacity")
            else:
                print(f"\n📊 Pool Summary:")
                print(f"   Healthy:    {len(healthy_pools)}")
                print(f"   Near-Full:  {len(near_full_pools)}")
                print(f"   Full:       {len(full_pools)}")
                print(f"   Offline:    {len(offline_pools)}")

        # Print disk capacity per node analysis
        node_drive_info = stc_data.get('node_drive_info', {})
        if node_drive_info:
            print(f"\n{'='*60}")
            print(f"NODE DISK CAPACITY ANALYSIS")
            print(f"{'='*60}")
            
            # Get provider from cloud storage info for max drives lookup
            provider = ''
            if cloud_storage_info:
                provider = (cloud_storage_info.get('provider') or '').lower()
            
            max_drives = config.max_drives_per_node.get(provider, config.default_max_drives_per_node)
            max_drives_per_pool = config.max_drives_per_pool
            
            print(f"\nPlatform: {provider.upper() if provider else 'Unknown (using default)'}")
            print(f"Max Drives per Node: {max_drives}")
            print(f"Max Drives per Pool: {max_drives_per_pool}")
            
            nodes_at_capacity = []
            nodes_near_capacity = []
            nodes_with_capacity = []
            
            print(f"\n{'Node':<40} {'Current':<10} {'Max':<8} {'Available':<10} {'Status'}")
            print(f"{'-'*40} {'-'*10} {'-'*8} {'-'*10} {'-'*15}")
            
            for node_name, drive_info in node_drive_info.items():
                if drive_info.get('error'):
                    print(f"{node_name[:40]:<40} {'Error':<10} {max_drives:<8} {'N/A':<10} ⚠️  Query failed")
                    continue
                    
                current_drives = drive_info.get('total_drives', 0)
                available_slots = max_drives - current_drives
                
                # Truncate node name for display
                display_name = node_name[:38] + '..' if len(node_name) > 40 else node_name
                
                if available_slots <= 0:
                    status = "🚫 AT CAPACITY"
                    nodes_at_capacity.append(node_name)
                elif available_slots <= 2:
                    status = "⚠️  LOW"
                    nodes_near_capacity.append(node_name)
                else:
                    status = "✅ OK"
                    nodes_with_capacity.append(node_name)
                
                print(f"{display_name:<40} {current_drives:<10} {max_drives:<8} {available_slots:<10} {status}")
            
            # Summary
            print(f"\n📊 Disk Slot Summary:")
            if nodes_at_capacity:
                print(f"\n🚫 NODES AT DISK CAPACITY ({len(nodes_at_capacity)}):")
                print(f"   WARNING: These nodes cannot attach additional drives")
                for node in nodes_at_capacity[:5]:
                    print(f"   - {node}")
                if len(nodes_at_capacity) > 5:
                    print(f"   ... and {len(nodes_at_capacity) - 5} more")
                print(f"\n   ✏️  CORRECTIVE ACTION:")
                print(f"   For StoreV2 migration, ensure nodes have available disk slots")
                print(f"   Consider removing unused drives or expanding to new nodes")
            
            if nodes_near_capacity:
                print(f"\n⚠️  NODES NEAR DISK CAPACITY ({len(nodes_near_capacity)}):")
                print(f"   These nodes have limited disk slots remaining")
                for node in nodes_near_capacity[:5]:
                    print(f"   - {node}")
                if len(nodes_near_capacity) > 5:
                    print(f"   ... and {len(nodes_near_capacity) - 5} more")
            
            if not nodes_at_capacity and not nodes_near_capacity:
                print(f"   ✅ All {len(nodes_with_capacity)} node(s) have sufficient disk slots available")
            else:
                print(f"\n   Summary:")
                print(f"   Nodes with capacity:     {len(nodes_with_capacity)}")
                print(f"   Nodes near capacity:     {len(nodes_near_capacity)}")
                print(f"   Nodes at capacity:       {len(nodes_at_capacity)}")

        # Print node CPU and memory resource analysis
        node_resources = stc_data.get('node_resources', {})
        if node_resources:
            print(f"\n{'='*60}")
            print(f"NODE CPU & MEMORY RESOURCE ANALYSIS")
            print(f"{'='*60}")
            
            min_cpu = config.storev2_min_cpu_cores
            min_mem = config.storev2_min_memory_gb
            rec_cpu = config.storev2_recommended_cpu_cores
            rec_mem = config.storev2_recommended_memory_gb
            
            print(f"\nStoreV2 Requirements:")
            print(f"  Minimum:     {min_cpu} CPU cores, {min_mem:.0f} GB RAM")
            print(f"  Recommended: {rec_cpu} CPU cores, {rec_mem:.0f} GB RAM")
            
            nodes_below_min = []
            nodes_below_recommended = []
            nodes_meets_recommended = []
            
            print(f"\n{'Node':<35} {'CPU':<8} {'Memory (GB)':<12} {'Status'}")
            print(f"{'-'*35} {'-'*8} {'-'*12} {'-'*20}")
            
            for node_name, resources in node_resources.items():
                capacity = resources.get('capacity', {})
                cpu = capacity.get('cpu', 0)
                mem_gb = capacity.get('memory_gb', 0)
                
                # Truncate node name for display
                display_name = node_name[:33] + '..' if len(node_name) > 35 else node_name
                
                if cpu < min_cpu or mem_gb < min_mem:
                    status = "🚫 BELOW MINIMUM"
                    nodes_below_min.append({
                        'name': node_name,
                        'cpu': cpu,
                        'memory_gb': mem_gb
                    })
                elif cpu < rec_cpu or mem_gb < rec_mem:
                    status = "⚠️  BELOW RECOMMENDED"
                    nodes_below_recommended.append({
                        'name': node_name,
                        'cpu': cpu,
                        'memory_gb': mem_gb
                    })
                else:
                    status = "✅ OK"
                    nodes_meets_recommended.append(node_name)
                
                print(f"{display_name:<35} {cpu:<8.0f} {mem_gb:<12.1f} {status}")
            
            # Summary
            print(f"\n📊 Resource Summary:")
            
            if nodes_below_min:
                print(f"\n🚫 NODES BELOW MINIMUM REQUIREMENTS ({len(nodes_below_min)}):")
                print(f"   CRITICAL: These nodes do not meet StoreV2 minimum requirements")
                for node in nodes_below_min[:5]:
                    issues = []
                    if node['cpu'] < min_cpu:
                        issues.append(f"CPU: {node['cpu']:.0f} < {min_cpu}")
                    if node['memory_gb'] < min_mem:
                        issues.append(f"Memory: {node['memory_gb']:.1f} GB < {min_mem:.0f} GB")
                    print(f"   - {node['name']}: {', '.join(issues)}")
                if len(nodes_below_min) > 5:
                    print(f"   ... and {len(nodes_below_min) - 5} more")
                print(f"\n   ✏️  CORRECTIVE ACTION:")
                print(f"   Upgrade nodes to meet minimum: {min_cpu} CPU cores, {min_mem:.0f} GB RAM")
                print(f"   Or migrate workloads to nodes with sufficient resources")
            
            if nodes_below_recommended:
                print(f"\n⚠️  NODES BELOW RECOMMENDED RESOURCES ({len(nodes_below_recommended)}):")
                print(f"   These nodes meet minimum but not recommended requirements")
                for node in nodes_below_recommended[:5]:
                    issues = []
                    if node['cpu'] < rec_cpu:
                        issues.append(f"CPU: {node['cpu']:.0f} < {rec_cpu}")
                    if node['memory_gb'] < rec_mem:
                        issues.append(f"Memory: {node['memory_gb']:.1f} GB < {rec_mem:.0f} GB")
                    print(f"   - {node['name']}: {', '.join(issues)}")
                if len(nodes_below_recommended) > 5:
                    print(f"   ... and {len(nodes_below_recommended) - 5} more")
            
            if not nodes_below_min and not nodes_below_recommended:
                print(f"   ✅ All {len(nodes_meets_recommended)} node(s) meet recommended resource requirements")
            else:
                print(f"\n   Summary:")
                print(f"   Meets recommended:     {len(nodes_meets_recommended)}")
                print(f"   Below recommended:     {len(nodes_below_recommended)}")
                print(f"   Below minimum:         {len(nodes_below_min)}")

        # Print pool priority analysis
        if pool_settings_inventory.get('pools'):
            print(f"\n{'='*60}")
            print(f"POOL PRIORITY & CONFIGURATION ANALYSIS")
            print(f"{'='*60}")

            # Inventory pool priorities
            priority_inventory = {}
            for pool_name, settings in pool_settings_inventory['pools'].items():
                priority = settings.get('priority', 'medium')
                if priority not in priority_inventory:
                    priority_inventory[priority] = []
                priority_inventory[priority].append(pool_name)

            print(f"\nPool Priority Distribution:")
            for priority in ['high', 'critical', 'medium', 'low']:
                if priority in priority_inventory:
                    pools = priority_inventory[priority]
                    print(f"  {priority.upper()}: {len(pools)} pool(s)")
                    if len(pools) <= 5:
                        for pool in pools:
                            print(f"    - {pool}")

            # Check StoreV2 compatibility
            print(f"\nStoreV2 Priority Mapping:")
            allowed_priorities = config.allowed_pool_priorities
            print(f"  Allowed StoreV2 Priorities: {', '.join(allowed_priorities)}")

            unsupported = []
            for priority in priority_inventory.keys():
                if priority not in allowed_priorities:
                    unsupported.append(priority)

            if unsupported:
                print(f"  ⚠️  UNSUPPORTED Priorities: {', '.join(unsupported)}")
                print(f"     These must be mapped to StoreV2 values before migration")
            else:
                print(f"  ✅ All priorities are compatible with StoreV2")

            # Non-default settings
            print(f"\nNon-Default Pool Settings (requiring manual reapplication):")
            has_non_default = False
            for pool_name, settings in pool_settings_inventory['pools'].items():
                non_default = {k: v for k, v in settings.items() if k != 'priority'}
                if non_default:
                    has_non_default = True
                    print(f"\n  Pool: {pool_name}")
                    for key, value in non_default.items():
                        print(f"    {key}: {value}")

            if not has_non_default:
                print(f"  ✅ No non-default settings detected")

        # Drive type analysis - disabled per user request
        # Drive type detection can be enabled in future versions if needed

        # Print migration action items
        print(f"\n{'='*60}")
        print(f"PRE-MIGRATION ACTION ITEMS")
        print(f"{'='*60}")

        action_items = []
        custom_label_list = []
        inconsistent_label_list = []

        # Check for custom pool labels (exclude .io/ labels AND Portworx system labels)
        if metadata_inventory.get('pool_labels'):
            custom_labels = {k: v for k, v in metadata_inventory['pool_labels'].items()
                           if '.io/' not in k and k not in config.portworx_system_labels}
            if custom_labels:
                custom_label_list = list(custom_labels.keys())
                action_items.append({
                    'text': f"Document and plan migration for {len(custom_labels)} custom pool label(s)",
                    'details': custom_label_list
                })

        # Check for inconsistent pool labels (include all labels for consistency check)
        inconsistent_labels_details = {}
        if metadata_inventory.get('pool_labels'):
            for label_key, values in metadata_inventory['pool_labels'].items():
                if len(values) > 1 and '.io/' not in label_key:
                    inconsistent_labels_details[label_key] = values

        if inconsistent_labels_details:
            inconsistent_label_list = list(inconsistent_labels_details.keys())
            action_items.append({
                'text': f"Resolve {len(inconsistent_label_list)} inconsistent pool label(s) across nodes",
                'details': inconsistent_label_list,
                'node_pool_details': inconsistent_labels_details
            })

        # Check for non-default pool settings
        if pool_settings_inventory.get('pools'):
            non_default_count = sum(1 for settings in pool_settings_inventory['pools'].values()
                                   if any(k != 'priority' for k in settings.keys()))
            if non_default_count > 0:
                action_items.append({
                    'text': f"Document {non_default_count} pool(s) with non-default settings for reapplication",
                    'details': None
                })

        # Check for unsupported priorities
        if pool_settings_inventory.get('pools'):
            unsupported_priorities = set()
            for settings in pool_settings_inventory['pools'].values():
                priority = settings.get('priority', 'medium')
                if priority not in config.allowed_pool_priorities:
                    unsupported_priorities.add(priority)
            if unsupported_priorities:
                action_items.append({
                    'text': f"Map {len(unsupported_priorities)} unsupported pool priorit{'y' if len(unsupported_priorities) == 1 else 'ies'} to StoreV2 values",
                    'details': list(unsupported_priorities)
                })

        if action_items:
            for i, item in enumerate(action_items, 1):
                print(f"  {i}. {item['text']}")
                if item.get('details'):
                    for detail in item['details']:
                        print(f"       - {detail}")
                # Show node:pool details for inconsistent labels
                if item.get('node_pool_details'):
                    for label_key, values in item['node_pool_details'].items():
                        print(f"         Label '{label_key}':")
                        for value, node_pool_list in values.items():
                            print(f"           Value '{value}':")
                            for node_pool in sorted(node_pool_list):
                                print(f"             - {node_pool}")
        else:
            print(f"  ✅ No metadata/configuration migration actions required")

        # Print per-node capacity details with migration mapping
        print(f"\n{'='*60}")
        print(f"PER-NODE MIGRATION CAPACITY MAPPING")
        print(f"{'='*60}")
        if nodes and sizing_recommendations and 'cluster' in sizing_recommendations:
            headroom_pct = sizing_recommendations['cluster']['headroom_percent']

            # Calculate free capacity for each node
            node_capacity_info = []
            for node_name, node_data in nodes.items():
                node_cap = node_data.get('capacity', {})
                total = node_cap.get('total', 0)
                used = node_cap.get('used', 0)
                free = total - used
                node_capacity_info.append({
                    'name': node_name,
                    'total': total,
                    'used': used,
                    'free': free,
                    'required_with_headroom': used * (1 + headroom_pct / 100)
                })

            print(f"\n  Format: Node | Used | Required (+{headroom_pct}% headroom) | Eligible Target Nodes")
            print(f"  " + "-"*76)

            for node_info in node_capacity_info:
                node_name = node_info['name']
                used_gb = node_info['used'] / (1024**3)
                required_gb = node_info['required_with_headroom'] / (1024**3)

                # Find nodes that can accept this node's data (have enough free capacity)
                eligible_targets = []
                for target in node_capacity_info:
                    if target['name'] != node_name:  # Can't migrate to self
                        if target['free'] >= node_info['required_with_headroom']:
                            eligible_targets.append(target['name'])

                # Truncate node name for display
                display_name = node_name[:35] + '...' if len(node_name) > 38 else node_name

                if eligible_targets:
                    target_count = len(eligible_targets)
                    status = f"✅ {target_count} node(s) available"
                else:
                    status = "⚠️  No eligible targets"

                print(f"\n  {display_name}")
                print(f"    Used: {used_gb:.1f} GB | Required: {required_gb:.1f} GB | {status}")

                if eligible_targets:
                    # Show up to 3 target nodes, truncate if more
                    if len(eligible_targets) <= 3:
                        targets_display = ", ".join(eligible_targets)
                    else:
                        targets_display = ", ".join(eligible_targets[:3]) + f" (+{len(eligible_targets)-3} more)"
                    print(f"    Eligible targets: {targets_display}")

            # Summary
            print(f"\n  " + "-"*76)
            total_nodes = len(node_capacity_info)

            nodes_fully_migratable = sum(1 for n in node_capacity_info
                                        if any(t['free'] >= n['required_with_headroom']
                                              for t in node_capacity_info if t['name'] != n['name']))

            if nodes_fully_migratable == total_nodes:
                print(f"\n  ✅ All {total_nodes} nodes have at least one eligible migration target")
            else:
                blocked = total_nodes - nodes_fully_migratable
                print(f"\n  ⚠️  {blocked} of {total_nodes} node(s) have no eligible migration target")
                print(f"      Consider expanding storage on potential target nodes")
        elif nodes:
            # Fallback to simple listing if no sizing recommendations
            for node_name, node_data in list(nodes.items())[:5]:
                node_cap = node_data.get('capacity', {})
                total_gb = node_cap.get('total', 0) / (1024**3)
                used_gb = node_cap.get('used', 0) / (1024**3)
                pool_count = len(node_data.get('pools', []))
                print(f"  {node_name}: {used_gb:.1f}/{total_gb:.1f} GB, {pool_count} pool(s)")

            if len(nodes) > 5:
                print(f"  ... and {len(nodes) - 5} more nodes")

        # Save JSON report if requested
        if args.output and not args.output.endswith('.txt'):
            report_data = {
                'timestamp': '2026-02-20T00:00:00Z',
                'executive_summary': exec_summary,
                'validation_results': [
                    {
                        'level': r.level.value,
                        'category': r.category,
                        'message': r.message,
                        'details': r.details,
                        'recommendations': r.recommendations
                    } for r in all_results
                ],
                'capacity_analysis': sizing_recommendations,
                'drive_conversion_plan': drive_conversion_plan,
                'metadata_inventory': metadata_inventory,
                'pool_settings_inventory': pool_settings_inventory,
                'detected_drive_types': detected_drive_types
            }

            with open(args.output, 'w') as f:
                json.dump(report_data, f, indent=2)

            logger.info(f"JSON report saved to {args.output}")

        # =====================================================================
        # FINAL SUMMARY - Quick Action Overview
        # =====================================================================
        print(f"\n{'='*70}")
        print(f"{'MIGRATION READINESS SUMMARY':^70}")
        print(f"{'='*70}")

        # Define all checks and their status
        checks_performed = []
        checks_passed = []
        checks_failed = []
        checks_warning = []
        checks_skipped = []

        # 0. License Check (critical - must be first)
        license_info = stc_data.get('license', {})
        checks_performed.append("License (Not Trial)")
        if license_info.get('is_trial'):
            checks_failed.append("License (Trial - BLOCKED)")
        elif license_info.get('type'):
            checks_passed.append("License (Not Trial)")
        else:
            checks_skipped.append("License (Not detected)")

        # 0b. Volume Attachments Check
        volume_attachments = stc_data.get('volume_attachments', {})
        checks_performed.append("Volume Attachments")
        if volume_attachments:
            is_trial = license_info.get('is_trial', False)
            attachment_limit = config.trial_volume_attachments_per_node if is_trial else config.licensed_volume_attachments_per_node
            
            nodes_over_limit = 0
            nodes_near_limit = 0
            for node_name, attach_info in volume_attachments.items():
                attached = attach_info.get('attached', 0)
                usage_pct = (attached / attachment_limit) * 100 if attachment_limit > 0 else 0
                
                if attached >= attachment_limit:
                    nodes_over_limit += 1
                elif usage_pct >= 80:
                    nodes_near_limit += 1
            
            if nodes_over_limit > 0:
                checks_warning.append("Volume Attachments (At Limit)")
            elif nodes_near_limit > 0:
                checks_warning.append("Volume Attachments (Near Limit)")
            else:
                checks_passed.append("Volume Attachments")
        else:
            checks_skipped.append("Volume Attachments (No data)")

        # Manual attachment check
        manually_attached = stc_data.get('manually_attached_volumes', {})
        checks_performed.append("Manual Volume Attachments")
        if manually_attached:
            checks_warning.append(f"Manual Volume Attachments ({len(manually_attached)} volume(s) need manual detach)")
        else:
            checks_passed.append("Manual Volume Attachments")

        # 1. Pod Health Check
        pod_health_issues = [r for r in all_results if r.category == 'Pod Health']
        checks_performed.append("Pod Health (Containers Ready)")
        if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in pod_health_issues):
            checks_failed.append("Pod Health (Containers Ready)")
        elif any(r.level == ValidationLevel.WARNING for r in pod_health_issues):
            checks_warning.append("Pod Health (Containers Ready)")
        else:
            checks_passed.append("Pod Health (Containers Ready)")

        # 1. Cluster Capacity Check
        capacity_issues = [r for r in all_results if r.category in ['Capacity Risk', 'Capacity Validation', 'Capacity Planning']]
        capacity_critical = any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in capacity_issues)
        checks_performed.append("Cluster Capacity")
        if capacity_critical:
            checks_failed.append("Cluster Capacity")
        elif any(r.level == ValidationLevel.WARNING for r in capacity_issues):
            checks_warning.append("Cluster Capacity")
        else:
            checks_passed.append("Cluster Capacity")

        # 2. Pool Health Check (offline/full pools)
        pool_health_issues = [r for r in all_results if r.category in ['Pool Health', 'Pool Capacity']]
        pool_health_critical = any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in pool_health_issues)
        checks_performed.append("Pool Health (Offline/Full)")
        if pool_health_critical:
            checks_failed.append("Pool Health (Offline/Full)")
        elif any(r.level == ValidationLevel.WARNING for r in pool_health_issues):
            checks_warning.append("Pool Health (Offline/Full)")
        else:
            checks_passed.append("Pool Health (Offline/Full)")

        # 3. Cloud Storage Drive Types
        cloud_issues = [r for r in all_results if r.category == 'Cloud Storage']
        if cloud_storage_info and cloud_storage_info.get('provider'):
            checks_performed.append("Cloud Storage Drive Types")
            if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in cloud_issues):
                checks_failed.append("Cloud Storage Drive Types")
            elif any(r.level == ValidationLevel.WARNING for r in cloud_issues):
                checks_warning.append("Cloud Storage Drive Types")
            else:
                checks_passed.append("Cloud Storage Drive Types")
        else:
            checks_skipped.append("Cloud Storage Drive Types (On-prem/N/A)")

        # 4. Pool Configuration (priorities)
        pool_config_issues = [r for r in all_results if r.category == 'Pool Configuration']
        checks_performed.append("Pool Configuration (Priorities)")
        if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in pool_config_issues):
            checks_failed.append("Pool Configuration (Priorities)")
        elif any(r.level == ValidationLevel.WARNING for r in pool_config_issues):
            checks_warning.append("Pool Configuration (Priorities)")
        else:
            checks_passed.append("Pool Configuration (Priorities)")

        # 5. Custom Labels/Metadata
        has_custom_labels = bool(metadata_inventory.get('pool_labels')) or bool(metadata_inventory.get('node_labels'))
        checks_performed.append("Custom Labels/Metadata")
        if has_custom_labels:
            # Check if there are custom labels that need migration (excluding system labels)
            custom_pool_labels_exist = any(
                '.io/' not in k and k not in config.portworx_system_labels
                for k in metadata_inventory.get('pool_labels', {}).keys()
            )
            if custom_pool_labels_exist:
                checks_warning.append("Custom Labels/Metadata (Action Required)")
            else:
                checks_passed.append("Custom Labels/Metadata")
        else:
            checks_passed.append("Custom Labels/Metadata")

        # 6. Data Integrity
        data_issues = [r for r in all_results if r.category == 'Data Integrity']
        checks_performed.append("Data Integrity")
        if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in data_issues):
            checks_failed.append("Data Integrity")
        else:
            checks_passed.append("Data Integrity")

        # 7. Cluster Size (minimum nodes check)
        cluster_size_issues = [r for r in all_results if r.category == 'Cluster Size']
        checks_performed.append("Cluster Size (Min Nodes)")
        if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in cluster_size_issues):
            checks_failed.append("Cluster Size (Min Nodes)")
        elif any(r.level == ValidationLevel.WARNING for r in cluster_size_issues):
            checks_warning.append("Cluster Size (Min Nodes)")
        else:
            checks_passed.append("Cluster Size (Min Nodes)")

        # 8. Metadata Node Labels (px/metadata-node)
        metadata_node_issues = [r for r in all_results if r.category == 'Metadata Node Labels']
        checks_performed.append("Metadata Node Labels")
        if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in metadata_node_issues):
            checks_failed.append("Metadata Node Labels")
        elif any(r.level == ValidationLevel.WARNING for r in metadata_node_issues):
            checks_warning.append("Metadata Node Labels")
        else:
            checks_passed.append("Metadata Node Labels")

        # 9. HA Reduction (repl-3 volumes on 3-node cluster)
        ha_issues = [r for r in all_results if r.category == 'HA Reduction']
        checks_performed.append("Volume HA During Migration")
        if any(r.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR] for r in ha_issues):
            checks_failed.append("Volume HA During Migration")
        elif any(r.level == ValidationLevel.WARNING for r in ha_issues):
            checks_warning.append("Volume HA During Migration")
        else:
            checks_passed.append("Volume HA During Migration")

        # 10. Node Disk Capacity (available disk slots)
        node_drive_info = stc_data.get('node_drive_info', {})
        checks_performed.append("Node Disk Capacity")
        if node_drive_info:
            # Get provider and max drives
            provider = ''
            if cloud_storage_info:
                provider = (cloud_storage_info.get('provider') or '').lower()
            max_drives = config.max_drives_per_node.get(provider, config.default_max_drives_per_node)
            
            # Check if any nodes are at or near capacity
            nodes_at_capacity = 0
            nodes_near_capacity = 0
            nodes_query_failed = 0
            for node_name, drive_info in node_drive_info.items():
                if drive_info.get('error'):
                    nodes_query_failed += 1
                    continue
                current_drives = drive_info.get('total_drives', 0)
                available_slots = max_drives - current_drives
                if available_slots <= 0:
                    nodes_at_capacity += 1
                elif available_slots <= 2:
                    nodes_near_capacity += 1

            if nodes_query_failed > 0:
                # Unable to determine capacity for some nodes - do not report
                # this check as passed/warning since the data is incomplete.
                checks_failed.append("Node Disk Capacity (Query Failed)")
            elif nodes_at_capacity > 0:
                checks_warning.append("Node Disk Capacity (At Limit)")
            elif nodes_near_capacity > 0:
                checks_warning.append("Node Disk Capacity (Near Limit)")
            else:
                checks_passed.append("Node Disk Capacity")
        else:
            checks_skipped.append("Node Disk Capacity (No data)")

        # 10. Node CPU/Memory Resources (StoreV2 requirements)
        node_resources = stc_data.get('node_resources', {})
        checks_performed.append("Node Resources (CPU/Memory)")
        if node_resources:
            min_cpu = config.storev2_min_cpu_cores
            min_mem = config.storev2_min_memory_gb
            rec_cpu = config.storev2_recommended_cpu_cores
            rec_mem = config.storev2_recommended_memory_gb
            
            nodes_below_min = 0
            nodes_below_recommended = 0
            for node_name, resources in node_resources.items():
                capacity = resources.get('capacity', {})
                cpu = capacity.get('cpu', 0)
                mem_gb = capacity.get('memory_gb', 0)
                
                if cpu < min_cpu or mem_gb < min_mem:
                    nodes_below_min += 1
                elif cpu < rec_cpu or mem_gb < rec_mem:
                    nodes_below_recommended += 1
            
            if nodes_below_min > 0:
                checks_failed.append("Node Resources (Below Minimum)")
            elif nodes_below_recommended > 0:
                checks_warning.append("Node Resources (Below Recommended)")
            else:
                checks_passed.append("Node Resources (CPU/Memory)")
        else:
            checks_skipped.append("Node Resources (No data)")

        # Print summary table
        print(f"\n┌{'─'*68}┐")
        print(f"│ {'CHECK':40} {'STATUS':25} │")
        print(f"├{'─'*68}┤")

        # Print passed checks
        for check in checks_passed:
            print(f"│ {check:40} {'✅ PASSED':25} │")

        # Print warning checks
        for check in checks_warning:
            print(f"│ {check:40} {'⚠️  WARNING':25} │")

        # Print failed checks
        for check in checks_failed:
            print(f"│ {check:40} {'❌ FAILED':25} │")

        # Print skipped checks
        for check in checks_skipped:
            print(f"│ {check:40} {'⏭️  SKIPPED':25} │")

        print(f"└{'─'*68}┘")

        # Quick stats
        total_checks = len(checks_performed)
        print(f"\n📊 QUICK STATS:")
        print(f"   Total Checks: {total_checks}")
        print(f"   ✅ Passed:    {len(checks_passed)}")
        print(f"   ⚠️  Warnings:  {len(checks_warning)}")
        print(f"   ❌ Failed:    {len(checks_failed)}")
        print(f"   ⏭️  Skipped:   {len(checks_skipped)}")

        # =====================================================================
        # DETAILED ACTION SUMMARY
        # =====================================================================
        print(f"\n{'='*70}")
        print(f"{'ACTION SUMMARY':^70}")
        print(f"{'='*70}")

        # Collect all blockers with their specific actions
        if by_level.get('CRITICAL') or by_level.get('ERROR'):
            print(f"\n🚨 BLOCKERS - Must be resolved before migration:")
            print(f"{'─'*70}")

            blocker_num = 1
            for result in all_results:
                if result.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR]:
                    print(f"\n  {blocker_num}. [{result.level.value}] {result.category}")
                    print(f"     Issue: {result.message}")
                    if result.recommendations:
                        print(f"     Actions:")
                        for rec in result.recommendations[:3]:  # Show top 3 recommendations
                            print(f"       → {rec}")
                    blocker_num += 1
        else:
            print(f"\n✅ No blocking issues found")

        # Collect all warnings with their specific actions
        # Check both ValidationResult warnings AND checks_warning list
        warning_results = [r for r in all_results if r.level == ValidationLevel.WARNING]

        if warning_results or checks_warning:
            print(f"\n⚠️  WARNINGS - Review and address if needed:")
            print(f"{'─'*70}")

            warning_num = 1

            # Print ValidationResult warnings first
            for result in warning_results:
                print(f"\n  {warning_num}. {result.category}")
                print(f"     Issue: {result.message}")
                if result.recommendations:
                    print(f"     Actions:")
                    for rec in result.recommendations[:2]:  # Show top 2 recommendations
                        print(f"       → {rec}")
                warning_num += 1

            # Print additional warnings from checks_warning that don't have ValidationResult
            # (e.g., Custom Labels/Metadata which is detected but not added as ValidationResult)
            warning_categories_from_results = {r.category for r in warning_results}
            for check_warning in checks_warning:
                # Check if this warning was already covered by a ValidationResult
                # Custom Labels/Metadata check won't have a ValidationResult
                if "Custom Labels" in check_warning and "Metadata Consistency" not in warning_categories_from_results:
                    print(f"\n  {warning_num}. Custom Labels/Metadata")
                    print(f"     Issue: Custom pool labels detected that require manual migration")
                    print(f"     Actions:")
                    print(f"       → Document custom labels before migration")
                    print(f"       → Reapply custom labels to new pools after migration")
                    # Build table of node/pool/label details
                    # Each row: (node_name, node_uuid, pool_name, pool_id, label_str)
                    table_rows = []
                    nodes_data = stc_data.get('status', {}).get('nodes', {})
                    pools_data = stc_data.get('status', {}).get('pools', {})
                    if metadata_inventory.get('pool_label_details'):
                        for detail in metadata_inventory['pool_label_details']:
                            if detail['label_key'] in config.portworx_system_labels:
                                continue
                            if '.io/' in detail['label_key']:
                                continue
                            label_str = f"{detail['label_key']}={detail['label_value']}"
                            # Strip node-scoped prefix from pool key (e.g. "pod:pool-0" -> "pool-0")
                            pool_display = detail['pool'].split(':', 1)[-1] if ':' in detail['pool'] else detail['pool']
                            # Look up node UUID from pxctl status
                            node_uuid = nodes_data.get(detail['node'], {}).get('id', '')
                            # Look up pool ID from pool data
                            pool_id = pools_data.get(detail['pool'], {}).get('id', '')
                            table_rows.append((detail['node'], node_uuid, pool_display, pool_id, label_str))
                    if table_rows:
                        # col1 fits both node name and [uuid] line
                        col1 = max(max(len(r[0]) for r in table_rows),
                                   max(len(f"[{r[1]}]") for r in table_rows),
                                   len("Node"))
                        col2 = max(len(r[2]) for r in table_rows)
                        col2 = max(col2, len("Pool"))
                        col3 = max(len(r[4]) for r in table_rows)
                        col3 = max(col3, len("Custom Labels"))
                        sep = f"     ┌{'─'*(col1+2)}┬{'─'*(col2+2)}┬{'─'*(col3+2)}┐"
                        hdr = f"     │ {'Node':^{col1}} │ {'Pool':^{col2}} │ {'Custom Labels':^{col3}} │"
                        mid = f"     ├{'─'*(col1+2)}┼{'─'*(col2+2)}┼{'─'*(col3+2)}┤"
                        bot = f"     └{'─'*(col1+2)}┴{'─'*(col2+2)}┴{'─'*(col3+2)}┘"
                        # Group rows by node, then by pool for vertical centering
                        # row: (node, node_uuid, pool, pool_id, label)
                        from itertools import groupby
                        node_groups = []
                        for node, node_rows in groupby(table_rows, key=lambda r: r[0]):
                            node_rows = list(node_rows)
                            node_uuid = node_rows[0][1]
                            pool_groups = []
                            for pool, pool_rows in groupby(node_rows, key=lambda r: r[2]):
                                pool_rows = list(pool_rows)
                                pool_id = pool_rows[0][3]
                                pool_groups.append((pool, pool_id, [r[4] for r in pool_rows]))
                            node_groups.append((node, node_uuid, pool_groups))

                        print(sep)
                        print(hdr)
                        print(mid)
                        for ni, (node, node_uuid, pool_groups) in enumerate(node_groups):
                            # Each node occupies 2 lines per row (name + [uuid]), so
                            # total_lines counts display lines for vertical centering
                            total_rows = sum(len(labels) for _, _, labels in pool_groups)
                            # Node name on first line, [uuid] on second — show at middle row pair
                            node_mid_row = total_rows // 2
                            row_idx = 0
                            for pi, (pool, pool_id, labels) in enumerate(pool_groups):
                                pool_mid_row = len(labels) // 2
                                if pi > 0:
                                    print(f"     │ {'':{col1}} ├{'─'*(col2+2)}┼{'─'*(col3+2)}┤")
                                for li, label in enumerate(labels):
                                    is_node_row = row_idx == node_mid_row
                                    display_node = node if is_node_row else ''
                                    display_pool = pool if li == pool_mid_row else ''
                                    if li > 0:
                                        print(f"     │ {'':{col1}} │ {'':{col2}} ├{'─'*(col3+2)}┤")
                                    print(f"     │ {display_node:^{col1}} │ {display_pool:^{col2}} │ {label:<{col3}} │")
                                    # Print [uuid] line directly below node name
                                    if is_node_row and node_uuid:
                                        uuid_str = f"[{node_uuid}]"
                                        print(f"     │ {uuid_str:^{col1}} │ {'':{col2}} │ {'':{col3}} │")
                                    row_idx += 1
                            if ni < len(node_groups) - 1:
                                print(mid)
                        print(bot)
                    warning_num += 1
                elif "Manual Volume Attachments" in check_warning:
                    manually_attached = stc_data.get('manually_attached_volumes', {})
                    print(f"\n  {warning_num}. Manual Volume Attachments")
                    print(f"     Issue: {len(manually_attached)} volume(s) attached via pxctl will not be detached automatically")
                    print(f"     Actions:")
                    print(f"       → Detach manually before migration: pxctl volume detach <vol>")
                    print(f"       → Failure to do so will cause drain-attachments to time out")
                    warning_num += 1
        else:
            print(f"\n✅ No warnings found")

        # Final verdict
        print(f"\n{'='*70}")

        # Exit with appropriate code
        # Check both validation results (by_level) and summary checks (checks_failed)
        has_critical_failures = by_level.get('CRITICAL') or by_level.get('ERROR') or checks_failed
        has_warnings = by_level.get('WARNING') or checks_warning
        
        if has_critical_failures:
            print(f"│ {'FINAL VERDICT: ❌ MIGRATION BLOCKED':^66} │")
            print(f"│ {'Address critical issues before proceeding':^66} │")
            print(f"{'─'*70}")
            sys.exit(1)
        elif has_warnings:
            print(f"│ {'FINAL VERDICT: ⚠️  PROCEED WITH CAUTION':^66} │")
            print(f"│ {'Review warnings and take corrective actions':^66} │")
            print(f"{'─'*70}")
            sys.exit(2)
        else:
            print(f"│ {'FINAL VERDICT: ✅ MIGRATION READY':^66} │")
            print(f"│ {'All validations passed - safe to proceed':^66} │")
            print(f"{'─'*70}")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()