# models/database.py
import psycopg2
from psycopg2.extras import DictCursor
from config.settings import settings
import contextlib
from typing import Optional, Tuple, Dict
import logging

class DatabaseConnection:
    @staticmethod
    @contextlib.contextmanager
    def get_connection():
        conn = None
        try:
            conn = psycopg2.connect(**settings.db_params)
            yield conn
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def test_connection() -> str:
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT version();')
                    version = cur.fetchone()
                    
                    # Check and setup monitoring capabilities
                    monitoring_status = DatabaseConnection.setup_monitoring()
                    return f"Successfully connected to PostgreSQL. Version: {version[0]}\n{monitoring_status}"
        except Exception as e:
            return f"Connection failed: {str(e)}"

    @staticmethod
    def setup_monitoring() -> str:
        """Attempt to set up monitoring capabilities and return status message"""
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if we have necessary permissions
                    cur.execute("SELECT usesuper, usecreatedb FROM pg_user WHERE usename = current_user;")
                    permissions = cur.fetchone()
                    
                    if not permissions:
                        return "Warning: Could not determine user permissions"
                    
                    is_superuser, can_create_db = permissions
                    
                    # Check if extension exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_available_extensions 
                            WHERE name = 'pg_stat_statements'
                        );
                    """)
                    extension_available = cur.fetchone()[0]
                    
                    if not extension_available:
                        return """
                            Note: pg_stat_statements extension is not available. 
                            Some monitoring features will be limited.
                            """
                    
                    # Try to create extension if not exists
                    try:
                        cur.execute("""
                            CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
                        """)
                        conn.commit()
                        return "Successfully configured monitoring capabilities"
                    except psycopg2.Error as e:
                        if "permission denied" in str(e).lower():
                            return """
                                Note: Limited permissions detected. 
                                Please contact your database administrator to enable pg_stat_statements.
                                Basic monitoring features will still be available.
                                """
                        return f"Warning: Could not enable full monitoring: {str(e)}"
                        
        except Exception as e:
            return f"Warning: Error during monitoring setup: {str(e)}"

    @staticmethod
    def get_basic_stats() -> Dict:
        """Get basic database statistics that don't require pg_stat_statements"""
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    # Get database size
                    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
                    db_size = cur.fetchone()[0]
                    
                    # Get table counts
                    cur.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
                    """)
                    table_count = cur.fetchone()[0]
                    
                    # Get active connections
                    cur.execute("""
                        SELECT count(*) FROM pg_stat_activity 
                        WHERE datname = current_database();
                    """)
                    connection_count = cur.fetchone()[0]
                    
                    return {
                        'database_size': db_size,
                        'table_count': table_count,
                        'active_connections': connection_count
                    }
        except Exception as e:
            logging.error(f"Error getting basic stats: {e}")
            return {}