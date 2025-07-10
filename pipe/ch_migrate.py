# ch_migrate.py
# Complete migration helper for ClickHouse data
# Automates the export from source and import to destination with deduplication

import argparse
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import subprocess
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickHouseMigrator:
    def __init__(self, export_script: str = "ch_export.py", import_script: str = "ch_import.py"):
        self.export_script = export_script
        self.import_script = import_script
        self.temp_dir = None
    
    def create_temp_directory(self) -> str:
        """Create temporary directory for migration files"""
        self.temp_dir = tempfile.mkdtemp(prefix="ch_migration_")
        logger.info(f"Created temporary directory: {self.temp_dir}")
        return self.temp_dir
    
    def cleanup_temp_directory(self):
        """Clean up temporary directory"""
        if self.temp_dir and Path(self.temp_dir).exists():
            import shutil
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
    
    def run_export(
        self,
        start_time: datetime,
        end_time: datetime,
        output_dir: str,
        symbol: Optional[str] = None,
        table: Optional[str] = None,
        batch_size: int = 100000
    ) -> Dict[str, Any]:
        """Run export script to generate Parquet files"""
        
        cmd = [
            sys.executable, self.export_script,
            "--start-time", start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "--end-time", end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "--output-dir", output_dir,
            "--batch-size", str(batch_size)
        ]
        
        if symbol:
            cmd.extend(["--symbol", symbol])
        
        if table:
            cmd.extend(["--table", table])
        
        logger.info(f"Running export command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("Export completed successfully")
            return {
                'success': True,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
        except subprocess.CalledProcessError as e:
            logger.error(f"Export failed with return code {e.returncode}")
            logger.error(f"stdout: {e.stdout}")
            logger.error(f"stderr: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stdout': e.stdout,
                'stderr': e.stderr
            }
    
    def run_import(
        self,
        directory: str,
        cutoff_time: Optional[datetime] = None,
        batch_size: int = 10000,
        check_existing: bool = True
    ) -> Dict[str, Any]:
        """Run import script to load Parquet files"""
        
        cmd = [
            sys.executable, self.import_script,
            "--directory", directory,
            "--batch-size", str(batch_size)
        ]
        
        if cutoff_time:
            cmd.extend(["--cutoff-time", cutoff_time.strftime('%Y-%m-%d %H:%M:%S')])
        
        if not check_existing:
            cmd.append("--no-check-existing")
        
        logger.info(f"Running import command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("Import completed successfully")
            return {
                'success': True,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
        except subprocess.CalledProcessError as e:
            logger.error(f"Import failed with return code {e.returncode}")
            logger.error(f"stdout: {e.stdout}")
            logger.error(f"stderr: {e.stderr}")
            return {
                'success': False,
                'error': str(e),
                'stdout': e.stdout,
                'stderr': e.stderr
            }
    
    def perform_migration(
        self,
        start_time: datetime,
        end_time: datetime,
        cutoff_time: Optional[datetime] = None,
        symbol: Optional[str] = None,
        table: Optional[str] = None,
        output_dir: Optional[str] = None,
        keep_files: bool = False,
        export_batch_size: int = 100000,
        import_batch_size: int = 10000
    ) -> Dict[str, Any]:
        """Perform complete migration process"""
        
        # Use temporary directory if no output directory specified
        if output_dir is None:
            output_dir = self.create_temp_directory()
            cleanup_after = not keep_files
        else:
            cleanup_after = False
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        migration_result = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'cutoff_time': cutoff_time.isoformat() if cutoff_time else None,
            'symbol': symbol,
            'table': table,
            'output_dir': output_dir,
            'export_result': None,
            'import_result': None,
            'success': False
        }
        
        try:
            # Step 1: Export data
            logger.info("=== STARTING EXPORT PHASE ===")
            export_result = self.run_export(
                start_time=start_time,
                end_time=end_time,
                output_dir=output_dir,
                symbol=symbol,
                table=table,
                batch_size=export_batch_size
            )
            
            migration_result['export_result'] = export_result
            
            if not export_result['success']:
                logger.error("Export failed, aborting migration")
                return migration_result
            
            # Check if any files were created
            parquet_files = list(Path(output_dir).glob("*.parquet"))
            if not parquet_files:
                logger.error("No Parquet files were created during export")
                migration_result['export_result']['error'] = "No Parquet files created"
                return migration_result
            
            logger.info(f"Export created {len(parquet_files)} Parquet files")
            
            # Step 2: Import data
            logger.info("=== STARTING IMPORT PHASE ===")
            import_result = self.run_import(
                directory=output_dir,
                cutoff_time=cutoff_time,
                batch_size=import_batch_size,
                check_existing=True
            )
            
            migration_result['import_result'] = import_result
            migration_result['success'] = import_result['success']
            
            if import_result['success']:
                logger.info("=== MIGRATION COMPLETED SUCCESSFULLY ===")
            else:
                logger.error("=== MIGRATION FAILED DURING IMPORT ===")
            
            return migration_result
        
        except Exception as e:
            logger.error(f"Migration failed with exception: {e}")
            migration_result['error'] = str(e)
            return migration_result
        
        finally:
            # Clean up temporary directory if needed
            if cleanup_after:
                self.cleanup_temp_directory()

def main():
    parser = argparse.ArgumentParser(description='Migrate ClickHouse data between instances')
    parser.add_argument('--start-time', type=str, required=True, help='Start time for migration (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end-time', type=str, required=True, help='End time for migration (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--cutoff-time', type=str, help='Cutoff time - do not import records newer than this (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--symbol', type=str, help='Symbol filter (e.g., ETH_USDT)')
    parser.add_argument('--table', type=str, help='Table to migrate (deals, klines, tickers, depth). If not specified, migrates all tables.')
    parser.add_argument('--output-dir', type=str, help='Directory to store Parquet files (default: temporary directory)')
    parser.add_argument('--keep-files', action='store_true', help='Keep Parquet files after migration')
    parser.add_argument('--export-batch-size', type=int, default=100000, help='Batch size for export operations')
    parser.add_argument('--import-batch-size', type=int, default=10000, help='Batch size for import operations')
    parser.add_argument('--last-days', type=int, help='Migrate data from last N days (alternative to start-time/end-time)')
    parser.add_argument('--cutoff-hours', type=int, help='Set cutoff time to N hours ago (alternative to cutoff-time)')
    
    args = parser.parse_args()
    
    # Parse time arguments
    if args.last_days:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=args.last_days)
    else:
        try:
            start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logger.error(f"Invalid time format: {e}")
            return
    
    # Parse cutoff time
    cutoff_time = None
    if args.cutoff_time:
        try:
            cutoff_time = datetime.strptime(args.cutoff_time, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logger.error(f"Invalid cutoff time format: {e}")
            return
    elif args.cutoff_hours:
        cutoff_time = datetime.now() - timedelta(hours=args.cutoff_hours)
    
    if start_time >= end_time:
        logger.error("Start time must be before end time")
        return
    
    # Display migration plan
    logger.info("=== MIGRATION PLAN ===")
    logger.info(f"Time range: {start_time} to {end_time}")
    if cutoff_time:
        logger.info(f"Cutoff time: {cutoff_time} (records newer than this will be preserved)")
    logger.info(f"Symbol filter: {args.symbol or 'All symbols'}")
    logger.info(f"Table filter: {args.table or 'All tables'}")
    logger.info(f"Output directory: {args.output_dir or 'Temporary directory'}")
    logger.info(f"Keep files: {args.keep_files}")
    logger.info()
    
    # Initialize migrator
    migrator = ClickHouseMigrator()
    
    # Perform migration
    result = migrator.perform_migration(
        start_time=start_time,
        end_time=end_time,
        cutoff_time=cutoff_time,
        symbol=args.symbol,
        table=args.table,
        output_dir=args.output_dir,
        keep_files=args.keep_files,
        export_batch_size=args.export_batch_size,
        import_batch_size=args.import_batch_size
    )
    
    # Display final results
    logger.info("=== MIGRATION SUMMARY ===")
    logger.info(f"Success: {result['success']}")
    logger.info(f"Time range: {result['start_time']} to {result['end_time']}")
    if result['cutoff_time']:
        logger.info(f"Cutoff time: {result['cutoff_time']}")
    logger.info(f"Output directory: {result['output_dir']}")
    
    if result['export_result']:
        logger.info("Export result: " + ("SUCCESS" if result['export_result']['success'] else "FAILED"))
    
    if result['import_result']:
        logger.info("Import result: " + ("SUCCESS" if result['import_result']['success'] else "FAILED"))
    
    if 'error' in result:
        logger.error(f"Migration error: {result['error']}")
    
    # Exit with appropriate code
    sys.exit(0 if result['success'] else 1)

if __name__ == "__main__":
    main()