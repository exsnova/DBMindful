# core/db_monitor.py
import pandas as pd
from models.database import DatabaseConnection

class DBMonitor:
    def __init__(self):
        self.db_connection = DatabaseConnection

# core/db_monitor.py
import pandas as pd
from models.database import DatabaseConnection
import logging
from typing import Optional, Dict

class DBMonitor:
    def __init__(self):
        self.db_connection = DatabaseConnection

    def get_heavy_queries(self) -> Optional[pd.DataFrame]:
        """Fetch the most resource-intensive queries with graceful fallback"""
        try:
            with self.db_connection.get_connection() as conn:
                # First check if pg_stat_statements is available
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
                        );
                    """)
                    has_pg_stat = cur.fetchone()[0]
                    
                    if not has_pg_stat:
                        # Fallback to basic query monitoring
                        query = """
                        SELECT 
                            query as query_preview,
                            pid as calls,
                            EXTRACT(EPOCH FROM (now() - query_start)) * 1000 as total_exec_time,
                            EXTRACT(EPOCH FROM (now() - query_start)) * 1000 as avg_exec_time,
                            0 as rows,
                            0 as total_blocks
                        FROM pg_stat_activity 
                        WHERE state = 'active'
                        AND query NOT LIKE '%pg_stat_activity%'
                        ORDER BY query_start DESC
                        LIMIT 10;
                        """
                    else:
                        # Use full pg_stat_statements query
                        query = """
                        SELECT 
                            query as query_preview,
                            calls,
                            total_exec_time,
                            total_exec_time/calls as avg_exec_time,
                            rows,
                            shared_blks_hit + shared_blks_read as total_blocks
                        FROM pg_stat_statements s
                        JOIN pg_database d ON d.oid = s.dbid
                        WHERE d.datname = current_database()
                        AND query NOT ILIKE 'BEGIN%'
                        AND query NOT ILIKE 'COMMIT%'
                        ORDER BY total_exec_time DESC
                        LIMIT 10;
                        """
                        
                return pd.read_sql(query, conn)
                
        except Exception as e:
            logging.error(f"Error fetching query data: {e}")
            return None

    def get_monitoring_status(self) -> Dict:
        """Get comprehensive monitoring status with basic stats"""
        try:
            # Get basic stats that don't require pg_stat_statements
            basic_stats = self.db_connection.get_basic_stats()
            
            # Try to get pg_stat_statements status
            with self.db_connection.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
                        );
                    """)
                    has_extension = cur.fetchone()[0]
                    
                    status = {
                        **basic_stats,
                        'pg_stat_statements_enabled': has_extension,
                        'monitoring_level': 'Full' if has_extension else 'Basic'
                    }
                    
                    if has_extension:
                        # Get additional pg_stat_statements specific info
                        cur.execute("SELECT pg_stat_statements_reset();")
                        status['stats_reset'] = True
                        
                    return status
                    
        except Exception as e:
            logging.error(f"Error checking monitoring status: {e}")
            return {'error': str(e)}

    def reset_query_stats(self):
        """Reset query statistics in pg_stat_statements"""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_stat_statements_reset();")
                conn.commit()

    def check_monitoring_prerequisites(self) -> dict:
        """Check if all prerequisites for query monitoring are met"""
        with self.db_connection.get_connection() as conn:
            with conn.cursor() as cur:
                results = {}
                
                try:
                    # Check if extension exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 
                            FROM pg_extension 
                            WHERE extname = 'pg_stat_statements'
                        );
                    """)
                    results['extension_installed'] = cur.fetchone()[0]
                    
                    # Check tracking settings
                    cur.execute("SHOW pg_stat_statements.track;")
                    results['track_setting'] = cur.fetchone()[0]
                    
                    cur.execute("SHOW pg_stat_statements.track_utility;")
                    results['track_utility'] = cur.fetchone()[0]
                    
                    # Check if statements are being tracked
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM pg_stat_statements 
                        WHERE query NOT ILIKE 'BEGIN%' 
                        AND query NOT ILIKE 'COMMIT%'
                    """)
                    results['tracked_queries'] = cur.fetchone()[0]
                    
                    # Get current configuration
                    cur.execute("SHOW shared_preload_libraries;")
                    results['shared_preload'] = cur.fetchone()[0]
                    
                except Exception as e:
                    results['error'] = str(e)
                
                return results

    def get_table_stats(self) -> pd.DataFrame:
        """Fetch table statistics including sequential reads"""
        with self.db_connection.get_connection() as conn:
            query = """
            SELECT
                schemaname || '.' || relname as table_name,
                seq_tup_read::bigint as row_count,
                n_dead_tup::bigint as dead_tuples,
                pg_total_relation_size(relid)::bigint as total_size,
                pg_relation_size(relid)::bigint as table_size,
                pg_indexes_size(relid)::bigint as index_size
            FROM pg_stat_user_tables
            ORDER BY seq_tup_read DESC;
            """
            return pd.read_sql(query, conn)

    def get_index_usage(self) -> pd.DataFrame:
        """Fetch index usage statistics"""
        with self.db_connection.get_connection() as conn:
            query = """
            SELECT
                schemaname || '.' || relname as table_name,
                indexrelname as index_name,
                idx_scan as number_of_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size
            FROM pg_stat_user_indexes
            ORDER BY idx_scan DESC;
            """
            return pd.read_sql(query, conn)