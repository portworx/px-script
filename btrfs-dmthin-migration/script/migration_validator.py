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

    def get_portworx_pods(self) -> List[str]:
        """Get list of Portworx pod names (Ready pods only)."""
        return [pod for pod, _node in self.get_portworx_pods_with_nodes()]

    def get_portworx_pods_with_nodes(self) -> List[Tuple[str, str]]:
        """Get [(pod_name, node_name), ...] for Ready Portworx pods.

        The nodeName lets callers filter to pods running on storage-capable nodes
        before fanning out expensive per-pod pxctl calls.
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

            if not pods_with_nodes:
                raise RuntimeError("No Ready Portworx pods found with label 'name=portworx'")

            logger.info(f"Found {len(pods_with_nodes)} Ready Portworx pods")
            return pods_with_nodes

        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to get Portworx pods: {e.stderr}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

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
                'cluster_uuid': None
            }
        }
        
        lines = pxctl_output.split('\n')
        
        # Parse cluster ID and UUID
        for line in lines:
            if 'Cluster ID:' in line:
                data['status']['cluster_id'] = line.split(':', 1)[1].strip()
            elif 'Cluster UUID:' in line:
                data['status']['cluster_uuid'] = line.split(':', 1)[1].strip()
        
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
                            data['status']['nodes'][node_name] = {
                                'id': node_id,
                                'ip': node_ip,
                                'capacity': {
                                    'used': self._parse_size(f"{used_val} {used_unit}"),
                                    'total': self._parse_size(f"{cap_val} {cap_unit}")
                                },
                                'status': status,
                                'labels': {},
                                'annotations': {},
                                'pools': []
                            }
                            nodes_parsed += 1
            
            if in_cluster_summary and 'Global Storage Pool' in line:
                break
        
        logger.info(f"Parsed {nodes_parsed} storage nodes from pxctl status")
        
        return data
    
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

            # Get Portworx pods with their node placement
            pods_with_nodes = self.get_portworx_pods_with_nodes()
            all_pods = [p for p, _ in pods_with_nodes]

            # Execute pxctl status on first available pod to get cluster-wide data
            logger.info("Collecting cluster-wide status...")
            pxctl_status_output = self.exec_pxctl_command(all_pods[0], ['status'])
            stc_data = self.parse_pxctl_status(pxctl_status_output)

            # Filter to pods running on storage-capable nodes. Storageless nodes have no
            # pools; querying them just wastes kubectl-exec RTTs. Match on both node
            # name and node IP because pxctl status may report either depending on the
            # cluster's label conventions.
            storage_node_names = set(stc_data['status']['nodes'].keys())
            storage_node_ips = {
                data.get('ip') for data in stc_data['status']['nodes'].values()
                if data.get('ip')
            }
            storage_pods = [
                pod for pod, node in pods_with_nodes
                if node in storage_node_names or node in storage_node_ips
            ]

            # Fallback safety: if the filter matched zero pods (e.g. node label scheme
            # doesn't line up), fall back to querying every pod. Better to be slow than
            # to skip real storage nodes.
            if storage_pods:
                pods = storage_pods
                logger.info(
                    f"Filtered {len(all_pods)} PX pods → {len(pods)} on storage nodes "
                    f"(skipping {len(all_pods) - len(pods)} storageless)"
                )
            else:
                pods = all_pods
                logger.warning(
                    f"Storage-node filter matched 0 pods; falling back to all {len(pods)} pods"
                )
            
            # Collect pool information from each node — fan out across pods in a bounded
            # thread pool. Each worker only runs the two kubectl execs (context-create + pool-show)
            # for one pod, then returns parsed pool dicts. Merging into stc_data happens serially
            # on the main thread after all workers complete, so no lock is needed for the merge.
            # Warm the auth-token cache once up front so worker threads don't race on it.
            self._detect_auth_token()

            workers = min(self.max_parallel, max(1, len(pods)))
            logger.info(
                f"Collecting pool information from {len(pods)} pod(s) "
                f"(parallel, up to {workers} concurrent)..."
            )

            pod_results: Dict[str, Dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_pod = {
                    executor.submit(self.exec_pxctl_command, pod, ['sv', 'pool', 'show']): pod
                    for pod in pods
                }
                completed = 0
                for future in as_completed(future_to_pod):
                    pod = future_to_pod[future]
                    completed += 1
                    try:
                        pool_output = future.result()
                    except Exception as e:
                        logger.warning(f"Failed to get pool info from pod {pod}: {e}")
                        continue
                    try:
                        pod_results[pod] = self.parse_pxctl_pool_show(pool_output, pod)
                    except Exception as e:
                        logger.warning(f"Failed to parse pool info from pod {pod}: {e}")
                        continue
                    if completed % 25 == 0 or completed == len(pods):
                        logger.info(f"  pool-show progress: {completed}/{len(pods)}")

            # Merge pool data serially (single-threaded, no lock needed)
            for pod, node_pools in pod_results.items():
                for pool_name, pool_data in node_pools.items():
                    stc_data['status']['pools'][pool_name] = pool_data

                    # Find matching node and add pool reference
                    for node_name, node_data in stc_data['status']['nodes'].items():
                        if pod in node_name or node_data.get('ip') in pool_data.get('node', ''):
                            node_data['pools'].append(pool_name)
                            # Copy pool labels to node
                            node_data['labels'].update(pool_data.get('labels', {}))
                            break
            
            logger.info("Successfully retrieved and parsed Portworx data")
            return stc_data
            
        except Exception as e:
            error_msg = f"Failed to retrieve STC data: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
    
    def validate_kubectl_access(self) -> bool:
        """Validate kubectl access"""
        try:
            # Check kubectl is available and can reach the cluster
            cmd = self._kubectl_base() + ['version', '--client']
            subprocess.run(cmd, capture_output=True, check=True, timeout=10)

            logger.info("kubectl access validated successfully")
            return True

        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.warning(f"kubectl validation failed: {e}")
            return False


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
        inventory = {'labels': {}, 'annotations': {}}
        
        items = [stc_data] if stc_data else []
        
        for item in items:
            # Get labels from POOLS (not nodes) - these are the storage pool labels
            pools = self._get_nested_field(item, 'status.pools') or {}
            
            for pool_name, pool_data in pools.items():
                # Inventory pool labels (these are the important ones for migration)
                pool_labels = pool_data.get('labels', {})
                
                for label_key, label_value in pool_labels.items():
                    if label_key not in inventory['labels']:
                        inventory['labels'][label_key] = {}
                    if label_value not in inventory['labels'][label_key]:
                        inventory['labels'][label_key][label_value] = []
                    inventory['labels'][label_key][label_value].append(pool_name)
            
            # Also check nodes for any node-level labels
            nodes = self._get_nested_field(item, 'status.nodes') or {}
            
            for node_name, node_data in nodes.items():
                # Inventory node annotations (if any)
                node_annotations = node_data.get('annotations', {})
                
                for annotation_key, annotation_value in node_annotations.items():
                    if annotation_key not in inventory['annotations']:
                        inventory['annotations'][annotation_key] = {}
                    if annotation_value not in inventory['annotations'][annotation_key]:
                        inventory['annotations'][annotation_key][annotation_value] = []
                    inventory['annotations'][annotation_key][annotation_value].append(node_name)
        
        return inventory
    
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
        if metadata_inventory['labels'] or metadata_inventory['annotations']:
            report_lines.extend([
                "CUSTOM METADATA REQUIRING MIGRATION",
                "-" * 40
            ])
            
            if metadata_inventory['labels']:
                report_lines.append("Labels:")
                for label_key, values in metadata_inventory['labels'].items():
                    report_lines.append(f"  {label_key}:")
                    for value, nodes in values.items():
                        report_lines.append(f"    {value}: {', '.join(nodes)}")
                report_lines.append("")
            
            if metadata_inventory['annotations']:
                report_lines.append("Annotations:")
                for annotation_key, values in metadata_inventory['annotations'].items():
                    report_lines.append(f"  {annotation_key}:")
                    for value, nodes in values.items():
                        report_lines.append(f"    {value}: {', '.join(nodes)}")
                report_lines.append("")
        
        # Non-Default Pool Settings
        if pool_settings_inventory['pools']:
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
        
        # Initialize validators
        sanity_checker = STCSanityChecker(config)
        capacity_analyzer = CapacityAnalyzer(config)
        metadata_checker = MetadataConsistencyChecker(config)
        pool_checker = PoolConfigurationChecker(config)
        reporter = MigrationReadinessReporter(config)
        
        # Run validations
        all_results = []
        
        # Sanity checks
        logger.info("Running sanity checks...")
        all_results.extend(sanity_checker.check_missing_fields(stc_data))
        all_results.extend(sanity_checker.check_zero_values(stc_data))
        all_results.extend(sanity_checker.check_missing_pools_nodes(stc_data))
        
        # Capacity analysis
        logger.info("Analyzing capacity requirements...")
        sizing_recommendations = capacity_analyzer.calculate_minimum_per_node_size(stc_data)
        all_results.extend(capacity_analyzer.check_per_node_feasibility(stc_data, sizing_recommendations))
        all_results.extend(capacity_analyzer.check_cluster_capacity_guardrails(stc_data))
        
        # Metadata consistency checks
        logger.info("Checking metadata consistency...")
        all_results.extend(metadata_checker.check_metadata_consistency(stc_data))
        metadata_inventory = metadata_checker.inventory_custom_metadata(stc_data)
        
        # Pool configuration checks
        logger.info("Validating pool configurations...")
        all_results.extend(pool_checker.check_pool_priorities(stc_data))
        all_results.extend(pool_checker.check_pool_distribution(stc_data))
        pool_settings_inventory = pool_checker.inventory_pool_settings(stc_data)
        
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
            print(f"  Total Storage Nodes: {len(nodes)}")
            print(f"  Total Storage Pools: {len(pools)}")
            
            # Pool distribution
            pools_per_node = {}
            for node_name, node_data in nodes.items():
                pool_count = len(node_data.get('pools', []))
                pools_per_node[node_name] = pool_count
            
            if pools_per_node:
                avg_pools = sum(pools_per_node.values()) / len(pools_per_node)
                print(f"  Avg Pools per Node:  {avg_pools:.1f}")
            
            # Sizing recommendations
            if sizing_recommendations and 'cluster' in sizing_recommendations:
                sizing = sizing_recommendations['cluster']
                print(f"\nCAPACITY SIZING RECOMMENDATIONS:")
                print(f"  Total Used:          {sizing['total_used']/(1024**3):,.1f} GB")
                print(f"  Storage Nodes:       {sizing['storage_node_count']}")
                print(f"  Min Per-Node:        {sizing['base_per_node_minimum']/(1024**3):,.1f} GB")
                print(f"  Recommended (+{sizing['headroom_percent']}%): {sizing['with_headroom']/(1024**3):,.1f} GB per node")
                
                # Calculate average available per node
                if nodes:
                    total_available = sum(node_data.get('capacity', {}).get('total', 0) for node_data in nodes.values())
                    avg_available_per_node = total_available / len(nodes)
                    print(f"\nPER-NODE CAPACITY COMPARISON:")
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
            
            # Executive Summary
            exec_summary = reporter.generate_executive_summary(all_results)
            print(f"\nVALIDATION SUMMARY:")
            print(f"  Status: {exec_summary['overall_status']}")
            print(f"  Risk Level: {exec_summary['risk_level']}")
            print(f"  Critical Blockers: {exec_summary['critical_blockers']}")
            print(f"  Warnings: {exec_summary['warnings']}")
            print(f"  Total Recommendations: {exec_summary['recommendations_count']}")
            
            if exec_summary['key_findings']:
                print(f"\n  Key Findings:")
                for i, finding in enumerate(exec_summary['key_findings'], 1):
                    print(f"    {i}. [{finding['impact']}] {finding['category']}: {finding['issue']}")
        
        # Group results by level for detailed display
        by_level = {}
        for result in all_results:
            level = result.level.value
            if level not in by_level:
                by_level[level] = []
            by_level[level].append(result)
        
        # Print detailed results for console output
        if not (args.output and args.output.endswith('.txt')):
            for level in ['CRITICAL', 'ERROR', 'WARNING']:
                results = by_level.get(level, [])
                if results:
                    print(f"\n{level} ISSUES:")
                    for i, result in enumerate(results, 1):
                        print(f"\n  {i}. [{result.category}] {result.message}")
                        if result.recommendations:
                            print(f"     Recommendations:")
                            for rec in result.recommendations:
                                print(f"       - {rec}")
        
        # Print metadata/labels inventory
        if metadata_inventory:
            print(f"\n{'='*60}")
            print(f"CUSTOM LABELS & METADATA ANALYSIS")
            print(f"{'='*60}")
            
            # Separate system labels from custom labels
            # Ignore all labels with .io/ (Kubernetes/vendor-specific labels)
            # Only track truly custom user-defined labels
            
            custom_labels_found = {}
            system_labels_found = {}
            hostname_labels_found = {}
            
            if metadata_inventory.get('labels'):
                for label_key, values in metadata_inventory['labels'].items():
                    # Check if it contains .io/ (system/vendor label)
                    is_system = '.io/' in label_key
                    
                    # Check if it's hostname-specific (appears to be node-specific)
                    is_hostname = 'hostname' in label_key.lower() or len(values) == len(nodes)
                    
                    if is_system:
                        system_labels_found[label_key] = values
                    elif is_hostname:
                        hostname_labels_found[label_key] = values
                    else:
                        custom_labels_found[label_key] = values
            
            # Log ignored system labels
            if system_labels_found:
                print(f"\nℹ️  IGNORED SYSTEM/VENDOR LABELS (containing .io/):")
                print(f"   The following labels are automatically managed and do not require migration:\n")
                for label_key in sorted(system_labels_found.keys()):
                    print(f"  - {label_key}")
            
            # Print custom labels that need migration
            if custom_labels_found:
                print(f"\n⚠️  CUSTOM LABELS REQUIRING MIGRATION:")
                print(f"   These labels must be manually applied to new storage pools post-migration:\n")
                for label_key, values in custom_labels_found.items():
                    print(f"  Label: {label_key}")
                    for value, nodes_list in values.items():
                        print(f"    Value '{value}': {len(nodes_list)} pool(s)")
                        if len(nodes_list) <= 3:
                            print(f"      Pools: {', '.join(nodes_list)}")
                print(f"\n  📋 ACTION REQUIRED: Document these labels and reapply after migration")
            else:
                print(f"\n✅ No custom labels detected - only system labels present")
            
            # Print hostname-specific labels warning
            if hostname_labels_found:
                print(f"\n📍 NODE-SPECIFIC LABELS (hostname-based):")
                print(f"   These labels are node-specific and will need updating for new nodes:\n")
                for label_key, values in hostname_labels_found.items():
                    print(f"  Label: {label_key}")
                    print(f"    {len(values)} unique value(s) across nodes")
                print(f"\n  📋 ACTION REQUIRED: Update hostname labels to match new node names")
            
            # Check for label consistency
            if custom_labels_found:
                print(f"\nCUSTOM LABEL CONSISTENCY CHECK:")
                consistent = True
                for label_key, values in custom_labels_found.items():
                    if len(values) > 1:
                        consistent = False
                        print(f"  ⚠️  INCONSISTENT: '{label_key}' has {len(values)} different values")
                        for value, nodes_list in values.items():
                            print(f"      '{value}': {len(nodes_list)} pool(s)")
                
                if consistent:
                    print(f"  ✅ All custom labels are consistent across pools")
            
            if metadata_inventory.get('annotations'):
                print(f"\nCUSTOM ANNOTATIONS DETECTED:")
                custom_annotations = {k: v for k, v in metadata_inventory['annotations'].items()
                                    if not any(k.startswith(prefix) for prefix in 
                                              ['kubernetes.io/', 'node.kubernetes.io/'])}
                
                if custom_annotations:
                    for annotation_key, values in custom_annotations.items():
                        print(f"\n  Annotation: {annotation_key}")
                        for value, nodes_list in values.items():
                            print(f"    Value: {value[:50]}{'...' if len(value) > 50 else ''}")
                            print(f"    Pools: {len(nodes_list)}")
                    print(f"\n  📋 ACTION REQUIRED: Document and reapply these annotations post-migration")
                else:
                    print(f"\n✅ No custom annotations detected")
                    if not any(annotation_key.startswith(prefix) for prefix in 
                              ['kubernetes.io/', 'node.kubernetes.io/']):
                        print(f"\n  Annotation: {annotation_key}")
                        for value, nodes in values.items():
                            print(f"    Value: {value[:50]}{'...' if len(value) > 50 else ''}")
                            print(f"    Nodes: {len(nodes)}")
        
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
        
        # Check for custom labels (exclude all .io/ labels)
        if metadata_inventory.get('labels'):
            custom_labels = {k: v for k, v in metadata_inventory['labels'].items() 
                           if '.io/' not in k}
            if custom_labels:
                custom_label_list = list(custom_labels.keys())
                action_items.append({
                    'text': f"Document and plan migration for {len(custom_labels)} custom label(s)",
                    'details': custom_label_list
                })
        
        # Check for inconsistent labels (exclude all .io/ labels)
        if metadata_inventory.get('labels'):
            for label_key, values in metadata_inventory['labels'].items():
                if len(values) > 1 and '.io/' not in label_key:
                    inconsistent_label_list.append(label_key)
        if inconsistent_label_list:
            action_items.append({
                'text': f"Resolve {len(inconsistent_label_list)} inconsistent label(s) across nodes",
                'details': inconsistent_label_list
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
        else:
            print(f"  ✅ No metadata/configuration migration actions required")
        
        # Print per-node capacity details
        print(f"\n{'='*60}")
        print(f"PER-NODE CAPACITY DETAILS")
        print(f"{'='*60}")
        if nodes:
            for node_name, node_data in list(nodes.items())[:5]:  # Show first 5 nodes
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
                'metadata_inventory': metadata_inventory,
                'pool_settings_inventory': pool_settings_inventory,
            }
            
            with open(args.output, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            logger.info(f"JSON report saved to {args.output}")
        
        # Exit with appropriate code
        if by_level.get('CRITICAL') or by_level.get('ERROR'):
            print(f"\n❌ Migration readiness: BLOCKED - Address critical issues before proceeding")
            sys.exit(1)
        elif by_level.get('WARNING'):
            print(f"\n⚠️  Migration readiness: PROCEED WITH CAUTION - Review warnings carefully")
            sys.exit(2)
        else:
            print(f"\n✅ Migration readiness: READY - All validations passed")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
