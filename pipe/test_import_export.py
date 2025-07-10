# test_import_export.py
# Comprehensive test script for import/export functionality verification

import os
import sys
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List
from clickhouse_driver import Client
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ImportExportTester:
    def __init__(self):
        self.client = self.connect_to_clickhouse()
        self.tables = ['deals', 'klines', 'tickers', 'depth']
        self.test_results = []
        
    def connect_to_clickhouse(self) -> Client:
        """Connect to ClickHouse"""
        try:
            client = Client(
                host=config.CLICKHOUSE_HOST,
                port=config.CLICKHOUSE_PORT,
                user=config.CLICKHOUSE_USER,
                password=config.CLICKHOUSE_PASSWORD,
                database=config.CLICKHOUSE_DB
            )
            client.execute('SELECT 1')
            logger.info("Connected to ClickHouse for testing")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def get_table_counts(self, time_filter: str = None) -> Dict[str, int]:
        """Get record counts for all tables"""
        counts = {}\n        for table in self.tables:
            query = f"SELECT COUNT(*) FROM {table}"
            if time_filter:
                query += f" WHERE {time_filter}"
            
            result = self.client.execute(query)
            counts[table] = result[0][0] if result else 0
        
        return counts
    
    def get_time_range(self, table: str) -> Dict[str, Any]:
        """Get time range for a table"""
        query = f"""
        SELECT 
            MIN(timestamp) as min_time,
            MAX(timestamp) as max_time,
            COUNT(*) as count
        FROM {table}
        """
        
        result = self.client.execute(query)
        if result and result[0]:
            return {
                'min_time': result[0][0],
                'max_time': result[0][1],
                'count': result[0][2]
            }
        return {'min_time': None, 'max_time': None, 'count': 0}
    
    def run_command(self, command: List[str], description: str) -> Dict[str, Any]:
        """Run a command and capture results"""
        logger.info(f"Running: {description}")
        logger.info(f"Command: {' '.join(command)}")
        
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=300)
            
            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'description': description
            }
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': 'Command timed out',
                'description': description
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'description': description
            }
    
    def test_phase_1_export_relative_time(self) -> Dict[str, Any]:
        """Test Phase 1: Export with relative time options"""
        logger.info("=== PHASE 1: TESTING RELATIVE TIME EXPORT ===")
        
        # Test 1: Export last 24 hours using --hours-back
        command = [
            sys.executable, 'ch_export.py',
            '--hours-back', '24',
            '--output-dir', './test_relative_export',
            '--symbol', 'ETH_USDT'
        ]
        
        result = self.run_command(command, "Export last 24 hours with --hours-back")
        
        if result['success']:
            # Verify files were created
            export_dir = Path('./test_relative_export')
            parquet_files = list(export_dir.glob('*.parquet'))
            
            result['files_created'] = len(parquet_files)
            result['file_list'] = [f.name for f in parquet_files]
            
            # Check file sizes
            total_size = sum(f.stat().st_size for f in parquet_files)
            result['total_size_bytes'] = total_size
            result['total_size_mb'] = total_size / (1024 * 1024)
        
        return result
    
    def test_phase_2_import_deduplication(self) -> Dict[str, Any]:
        """Test Phase 2: Import with full deduplication"""
        logger.info("=== PHASE 2: TESTING IMPORT DEDUPLICATION ===")
        
        # Get baseline counts
        baseline_counts = self.get_table_counts()
        
        # Test import of existing data (should be 100% duplicates)
        command = [
            sys.executable, 'ch_import.py',
            '--directory', './test_export'
        ]\n        
        result = self.run_command(command, "Import existing data (deduplication test)")
        
        if result['success']:
            # Check counts after import
            after_counts = self.get_table_counts()
            
            # Should be identical (no new records)
            result['baseline_counts'] = baseline_counts
            result['after_counts'] = after_counts
            result['records_added'] = {table: after_counts[table] - baseline_counts[table] for table in self.tables}
            result['perfect_deduplication'] = all(added == 0 for added in result['records_added'].values())
        
        return result
    
    def test_phase_3_sequential_cutoff(self) -> Dict[str, Any]:
        """Test Phase 3: Sequential cutoff calculation"""
        logger.info("=== PHASE 3: TESTING SEQUENTIAL CUTOFF ===")
        
        # Test import with auto-cutoff enabled
        command = [
            sys.executable, 'ch_import.py',
            '--directory', './test_export',
            '--no-check-existing'  # Skip existence check to see cutoff in action
        ]
        
        result = self.run_command(command, "Test automatic sequential cutoff")
        
        # Parse output for cutoff information
        if result['success'] and result['stdout']:
            cutoff_info = {}\n            for line in result['stdout'].split('\\n'):
                if 'Sequential cutoff calculated' in line:
                    # Extract table and cutoff time
                    parts = line.split(': ')
                    if len(parts) >= 2:
                        table_info = parts[1].split(' cutoff calculated for ')
                        if len(table_info) >= 2:
                            table = table_info[1].split(':')[0]
                            cutoff_info[table] = parts[2] if len(parts) > 2 else 'parsed'
            
            result['cutoff_calculations'] = cutoff_info
        
        return result
    
    def test_phase_4_gap_fill(self) -> Dict[str, Any]:
        """Test Phase 4: Gap-fill functionality"""
        logger.info("=== PHASE 4: TESTING GAP-FILL FUNCTIONALITY ===")
        
        # First, we need to create a gap - delete 1 hour of data
        test_start = datetime(2025, 7, 4, 20, 51, 20)
        test_end = datetime(2025, 7, 4, 21, 51, 20)
        
        logger.info(f"Creating data gap: {test_start} to {test_end}")
        
        # Delete records from all tables
        deletion_counts = {}
        for table in self.tables:
            delete_query = f"""
            DELETE FROM {table}
            WHERE timestamp >= '{test_start.strftime('%Y-%m-%d %H:%M:%S')}'
            AND timestamp <= '{test_end.strftime('%Y-%m-%d %H:%M:%S')}'
            AND symbol = 'ETH_USDT'
            """
            
            try:
                self.client.execute(delete_query)
                
                # Count how many were deleted by checking the gap
                count_query = f"""
                SELECT COUNT(*) FROM {table}
                WHERE timestamp >= '{test_start.strftime('%Y-%m-%d %H:%M:%S')}'
                AND timestamp <= '{test_end.strftime('%Y-%m-%d %H:%M:%S')}'
                AND symbol = 'ETH_USDT'
                """
                
                result_count = self.client.execute(count_query)
                remaining_count = result_count[0][0] if result_count else 0
                
                deletion_counts[table] = {'remaining_in_gap': remaining_count}
                logger.info(f"Deleted from {table}, remaining in gap: {remaining_count}")
                
            except Exception as e:
                logger.error(f"Failed to delete from {table}: {e}")
                deletion_counts[table] = {'error': str(e)}
        
        # Now test gap-fill import
        baseline_counts = self.get_table_counts()
        
        command = [
            sys.executable, 'ch_import.py',
            '--directory', './test_export'
        ]
        
        import_result = self.run_command(command, "Gap-fill import test")
        
        if import_result['success']:
            after_counts = self.get_table_counts()
            
            import_result['baseline_counts'] = baseline_counts
            import_result['after_counts'] = after_counts
            import_result['records_added'] = {table: after_counts[table] - baseline_counts[table] for table in self.tables}
            import_result['deletion_info'] = deletion_counts
        
        return import_result
    
    def run_comprehensive_test(self) -> Dict[str, Any]:
        """Run all test phases"""
        logger.info("=== STARTING COMPREHENSIVE IMPORT/EXPORT TEST ===")
        
        test_results = {
            'start_time': datetime.now(),
            'phases': {}
        }
        
        # Phase 1: Export with relative time
        test_results['phases']['phase_1_export'] = self.test_phase_1_export_relative_time()
        
        # Phase 2: Import deduplication
        test_results['phases']['phase_2_deduplication'] = self.test_phase_2_import_deduplication()
        
        # Phase 3: Sequential cutoff
        test_results['phases']['phase_3_cutoff'] = self.test_phase_3_sequential_cutoff()
        
        # Phase 4: Gap-fill
        test_results['phases']['phase_4_gap_fill'] = self.test_phase_4_gap_fill()
        
        test_results['end_time'] = datetime.now()
        test_results['total_duration'] = (test_results['end_time'] - test_results['start_time']).total_seconds()
        
        return test_results
    
    def print_test_summary(self, results: Dict[str, Any]):
        """Print comprehensive test summary"""
        logger.info("=== COMPREHENSIVE TEST SUMMARY ===")
        logger.info(f"Test duration: {results['total_duration']:.1f} seconds")
        
        for phase_name, phase_result in results['phases'].items():
            logger.info(f"\\n{phase_name.upper().replace('_', ' ')}:")
            logger.info(f"  Success: {phase_result['success']}")
            
            if phase_result['success']:
                if 'files_created' in phase_result:
                    logger.info(f"  Files created: {phase_result['files_created']}")
                    logger.info(f"  Total size: {phase_result.get('total_size_mb', 0):.1f} MB")
                
                if 'perfect_deduplication' in phase_result:
                    logger.info(f"  Perfect deduplication: {phase_result['perfect_deduplication']}")
                    logger.info(f"  Records added: {phase_result['records_added']}")
                
                if 'cutoff_calculations' in phase_result:
                    logger.info(f"  Cutoff calculations: {len(phase_result['cutoff_calculations'])} tables")
                
                if 'records_added' in phase_result and phase_name == 'phase_4_gap_fill':
                    total_added = sum(phase_result['records_added'].values())
                    logger.info(f"  Gap-fill records added: {total_added}")
            
            else:
                logger.error(f"  Error: {phase_result.get('error', 'Unknown error')}")
                if 'stderr' in phase_result:
                    logger.error(f"  Stderr: {phase_result['stderr'][:200]}...")

def main():
    """Main test execution"""
    logger.info("Starting comprehensive import/export functionality test")
    
    # Create test directories
    Path('./test_relative_export').mkdir(exist_ok=True)
    
    # Initialize tester
    tester = ImportExportTester()
    
    # Run tests
    results = tester.run_comprehensive_test()
    
    # Print summary
    tester.print_test_summary(results)
    
    # Determine overall success
    all_phases_success = all(phase['success'] for phase in results['phases'].values())
    
    if all_phases_success:
        logger.info("\\n[SUCCESS] All test phases completed successfully!")
        sys.exit(0)
    else:
        logger.error("\\n[FAILURE] Some test phases failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()