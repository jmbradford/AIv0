#!/usr/bin/env python3
"""
Resource monitoring script to track CPU, memory, and system usage
for the MEXC data pipeline baseline performance testing.
"""

import time
import json
import psutil
import logging
from datetime import datetime
import subprocess
import os
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('resource_baseline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ResourceMonitor:
    def __init__(self, monitor_interval=10):
        self.monitor_interval = monitor_interval
        self.start_time = datetime.now()
        self.baseline_log = []
        
    def get_process_stats(self, process_name):
        """Get resource usage for a specific process"""
        stats = {
            'cpu_percent': 0,
            'memory_mb': 0,
            'memory_percent': 0,
            'num_threads': 0,
            'num_fds': 0,
            'status': 'not_found'
        }
        
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info', 'memory_percent', 'num_threads', 'num_fds', 'status']):
                if process_name in proc.info['name']:
                    stats['cpu_percent'] += proc.info['cpu_percent'] or 0
                    stats['memory_mb'] += (proc.info['memory_info'].rss / 1024 / 1024) if proc.info['memory_info'] else 0
                    stats['memory_percent'] += proc.info['memory_percent'] or 0
                    stats['num_threads'] += proc.info['num_threads'] or 0
                    stats['num_fds'] += proc.info['num_fds'] or 0
                    stats['status'] = 'running'
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
        return stats
    
    def get_docker_stats(self):
        """Get Docker container resource usage"""
        docker_stats = {}
        
        try:
            # Get container stats
            containers = ['kafka', 'clickhouse', 'zookeeper']
            for container in containers:
                try:
                    result = subprocess.run(
                        ['docker', 'stats', container, '--no-stream', '--format', 'table {{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}'],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        if len(lines) > 1:
                            stats_line = lines[1].split('\t')
                            if len(stats_line) >= 4:
                                docker_stats[container] = {
                                    'cpu_percent': stats_line[0],
                                    'memory_usage': stats_line[1],
                                    'network_io': stats_line[2],
                                    'block_io': stats_line[3]
                                }
                except subprocess.TimeoutExpired:
                    docker_stats[container] = {'status': 'timeout'}
                except Exception as e:
                    docker_stats[container] = {'status': f'error: {str(e)}'}
                    
        except Exception as e:
            logger.error(f"Error getting Docker stats: {e}")
            
        return docker_stats
    
    def get_kafka_topic_sizes(self):
        """Get Kafka topic sizes and partition info"""
        kafka_stats = {}
        
        try:
            # Get topic sizes using docker exec
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'sh', '-c',
                'ls -la /var/lib/kafka/data/mexc_* | head -20'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                kafka_stats['topic_dirs'] = result.stdout.strip()
                
            # Get topic info
            result = subprocess.run([
                'docker', 'exec', 'kafka', 'kafka-topics',
                '--bootstrap-server', 'localhost:9092',
                '--list'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                kafka_stats['topics'] = result.stdout.strip().split('\n')
                
        except Exception as e:
            logger.error(f"Error getting Kafka stats: {e}")
            kafka_stats['error'] = str(e)
            
        return kafka_stats
    
    def get_clickhouse_stats(self):
        """Get ClickHouse table row counts and sizes"""
        clickhouse_stats = {}
        
        try:
            # Get table row counts
            result = subprocess.run([
                'docker', 'exec', 'clickhouse', 'clickhouse-client', '-q',
                'SELECT table, total_rows, total_bytes FROM system.tables WHERE database = \'mexc_data\' AND table IN (\'deals\', \'klines\', \'tickers\', \'depth\') FORMAT JSON'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                clickhouse_stats['table_stats'] = json.loads(result.stdout)
                
        except Exception as e:
            logger.error(f"Error getting ClickHouse stats: {e}")
            clickhouse_stats['error'] = str(e)
            
        return clickhouse_stats
    
    def get_system_stats(self):
        """Get overall system resource usage"""
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'memory_available_mb': psutil.virtual_memory().available / 1024 / 1024,
            'memory_used_mb': psutil.virtual_memory().used / 1024 / 1024,
            'memory_total_mb': psutil.virtual_memory().total / 1024 / 1024,
            'swap_percent': psutil.swap_memory().percent,
            'disk_usage_percent': psutil.disk_usage('/').percent,
            'load_avg': os.getloadavg() if hasattr(os, 'getloadavg') else [0, 0, 0],
            'boot_time': datetime.fromtimestamp(psutil.boot_time()).isoformat()
        }
    
    def collect_metrics(self):
        """Collect all metrics and return as dictionary"""
        timestamp = datetime.now().isoformat()
        runtime_seconds = (datetime.now() - self.start_time).total_seconds()
        
        metrics = {
            'timestamp': timestamp,
            'runtime_seconds': runtime_seconds,
            'system': self.get_system_stats(),
            'processes': {
                'python': self.get_process_stats('python'),
                'client.py': self.get_process_stats('client.py'),
            },
            'docker': self.get_docker_stats(),
            'kafka': self.get_kafka_topic_sizes(),
            'clickhouse': self.get_clickhouse_stats()
        }
        
        return metrics
    
    def log_metrics(self, metrics):
        """Log metrics to file and console"""
        self.baseline_log.append(metrics)
        
        # Log summary to console
        logger.info(f"Runtime: {metrics['runtime_seconds']:.0f}s | "
                   f"System CPU: {metrics['system']['cpu_percent']:.1f}% | "
                   f"Memory: {metrics['system']['memory_percent']:.1f}% | "
                   f"Python Memory: {metrics['processes']['python']['memory_mb']:.1f}MB")
        
        # Log full metrics to file
        with open('resource_baseline.log', 'a') as f:
            f.write(f"METRICS: {json.dumps(metrics)}\n")
    
    def run_monitoring(self, duration_minutes=60):
        """Run monitoring for specified duration"""
        logger.info(f"Starting resource monitoring for {duration_minutes} minutes")
        logger.info(f"Monitoring interval: {self.monitor_interval} seconds")
        
        end_time = datetime.now().timestamp() + (duration_minutes * 60)
        
        try:
            while datetime.now().timestamp() < end_time:
                try:
                    metrics = self.collect_metrics()
                    self.log_metrics(metrics)
                    time.sleep(self.monitor_interval)
                except KeyboardInterrupt:
                    logger.info("Monitoring interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Error during monitoring: {e}")
                    time.sleep(self.monitor_interval)
                    
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        
        logger.info(f"Monitoring completed. Total samples: {len(self.baseline_log)}")
        self.generate_summary()
    
    def generate_summary(self):
        """Generate summary statistics from collected metrics"""
        if not self.baseline_log:
            logger.warning("No metrics collected for summary")
            return
        
        # Calculate averages and peaks
        cpu_values = [m['system']['cpu_percent'] for m in self.baseline_log]
        memory_values = [m['system']['memory_percent'] for m in self.baseline_log]
        python_memory_values = [m['processes']['python']['memory_mb'] for m in self.baseline_log]
        
        summary = {
            'total_runtime_minutes': self.baseline_log[-1]['runtime_seconds'] / 60,
            'total_samples': len(self.baseline_log),
            'cpu_stats': {
                'avg': sum(cpu_values) / len(cpu_values),
                'max': max(cpu_values),
                'min': min(cpu_values)
            },
            'memory_stats': {
                'avg': sum(memory_values) / len(memory_values),
                'max': max(memory_values),
                'min': min(memory_values)
            },
            'python_memory_stats': {
                'avg': sum(python_memory_values) / len(python_memory_values),
                'max': max(python_memory_values),
                'min': min(python_memory_values)
            }
        }
        
        logger.info("=== BASELINE PERFORMANCE SUMMARY ===")
        logger.info(f"Total Runtime: {summary['total_runtime_minutes']:.1f} minutes")
        logger.info(f"CPU Usage - Avg: {summary['cpu_stats']['avg']:.1f}% | Max: {summary['cpu_stats']['max']:.1f}%")
        logger.info(f"Memory Usage - Avg: {summary['memory_stats']['avg']:.1f}% | Max: {summary['memory_stats']['max']:.1f}%")
        logger.info(f"Python Memory - Avg: {summary['python_memory_stats']['avg']:.1f}MB | Max: {summary['python_memory_stats']['max']:.1f}MB")
        
        # Write summary to file
        with open('resource_baseline_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info("Summary saved to resource_baseline_summary.json")

def main():
    """Main function to run resource monitoring"""
    monitor = ResourceMonitor(monitor_interval=10)
    
    # Default to 60 minutes, but can be overridden
    duration = 60
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid duration argument, using default 60 minutes")
    
    monitor.run_monitoring(duration_minutes=duration)

if __name__ == "__main__":
    main()