"""
Data extraction modules for DeFtunes pipeline.

Handles extraction from API endpoints and database sources.
"""

import json
import time
import requests
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

from .config import settings
from .utils import get_logger, retry, timer, APIError, DatabaseError


class APIExtractor:
    """Extract data from DeFtunes API endpoints."""
    
    def __init__(self):
        self.logger = get_logger("api_extractor")
        self.base_url = settings.api.base_url
        self.timeout = settings.api.timeout
        self.max_retries = settings.api.max_retries
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration."""
        session = requests.Session()
        
        # Configure session
        session.timeout = self.timeout
        session.headers.update({
            'User-Agent': 'DeFtunes-Pipeline/1.0',
            'Accept': 'application/json',
        })
        
        return session
    
    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make HTTP request with retry logic."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params or {})
            response.raise_for_status()
            
            self.logger.info(f"API request successful: {endpoint}", extra={
                'endpoint': endpoint,
                'status_code': response.status_code,
                'response_time': response.elapsed.total_seconds()
            })
            
            return response.json()
            
        except requests.RequestException as e:
            self.logger.error(f"API request failed: {endpoint}", extra={
                'endpoint': endpoint,
                'error': str(e)
            })
            raise APIError(f"API request failed for {endpoint}: {str(e)}")
    
    def extract_users(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Extract user data from API.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of user records
        """
        with timer(f"Extracting users from {start_date} to {end_date}", self.logger):
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            
            data = self._make_request('users', params)
            
            # Handle different response formats
            if isinstance(data, dict):
                users = data.get('users', data.get('data', []))
            else:
                users = data
            
            self.logger.info(f"Extracted {len(users)} users", extra={
                'start_date': start_date,
                'end_date': end_date,
                'record_count': len(users)
            })
            
            return users
    
    def extract_sessions(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Extract session data from API.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of session records
        """
        with timer(f"Extracting sessions from {start_date} to {end_date}", self.logger):
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            
            data = self._make_request('sessions', params)
            
            # Handle different response formats
            if isinstance(data, dict):
                sessions = data.get('sessions', data.get('data', []))
            else:
                sessions = data
            
            self.logger.info(f"Extracted {len(sessions)} sessions", extra={
                'start_date': start_date,
                'end_date': end_date,
                'record_count': len(sessions)
            })
            
            return sessions
    
    def validate_api_connection(self) -> bool:
        """
        Validate API connection.
        
        Returns:
            True if connection is successful
        """
        try:
            # Try to make a simple request
            self._make_request('health')
            return True
        except APIError:
            return False


class DatabaseExtractor:
    """Extract data from PostgreSQL database."""
    
    def __init__(self):
        self.logger = get_logger("database_extractor")
        self.config = settings.database
        
    def _get_connection(self):
        """Get database connection."""
        try:
            connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password
            )
            return connection
        except psycopg2.Error as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise DatabaseError(f"Database connection failed: {str(e)}")
    
    @retry(max_attempts=3, delay=2.0, backoff=2.0)
    def extract_songs(self, batch_size: int = 1000) -> List[Dict]:
        """
        Extract songs data from database.
        
        Args:
            batch_size: Number of records to fetch at once
            
        Returns:
            List of song records
        """
        with timer("Extracting songs from database", self.logger):
            connection = self._get_connection()
            
            try:
                cursor = connection.cursor(cursor_factory=RealDictCursor)
                
                # Get total count first
                cursor.execute("SELECT COUNT(*) FROM deftunes.songs")
                total_count = cursor.fetchone()[0]
                
                # Extract data in batches
                all_songs = []
                offset = 0
                
                while offset < total_count:
                    query = """
                        SELECT 
                            song_id,
                            title,
                            artist_name,
                            album_name,
                            duration,
                            year,
                            genre,
                            artist_familiarity,
                            artist_hotttnesss,
                            track_7digitalid,
                            release_7digitalid
                        FROM deftunes.songs
                        ORDER BY song_id
                        LIMIT %s OFFSET %s
                    """
                    
                    cursor.execute(query, (batch_size, offset))
                    batch = cursor.fetchall()
                    
                    # Convert to list of dicts
                    batch_dicts = [dict(row) for row in batch]
                    all_songs.extend(batch_dicts)
                    
                    offset += batch_size
                    
                    self.logger.info(f"Extracted batch: {len(batch_dicts)} records (total: {len(all_songs)})")
                
                self.logger.info(f"Extracted {len(all_songs)} songs from database", extra={
                    'record_count': len(all_songs),
                    'batch_size': batch_size
                })
                
                return all_songs
                
            finally:
                connection.close()
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics for a database table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics
        """
        connection = self._get_connection()
        
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Get basic stats
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as record_count,
                    pg_size_pretty(pg_total_relation_size('{table_name}')) as table_size
                FROM {table_name}
            """)
            
            stats = dict(cursor.fetchone())
            
            # Get column info
            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable
                FROM information_schema.columns 
                WHERE table_name = '{table_name.split('.')[-1]}'
                ORDER BY ordinal_position
            """)
            
            columns = [dict(row) for row in cursor.fetchall()]
            stats['columns'] = columns
            
            return stats
            
        finally:
            connection.close()
    
    def validate_database_connection(self) -> bool:
        """
        Validate database connection.
        
        Returns:
            True if connection is successful
        """
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            connection.close()
            return True
        except DatabaseError:
            return False


def extract_incremental_data(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Extract incremental data from all sources.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dictionary containing extracted data from all sources
    """
    logger = get_logger("incremental_extractor")
    
    # Initialize extractors
    api_extractor = APIExtractor()
    db_extractor = DatabaseExtractor()
    
    # Validate connections
    if not api_extractor.validate_api_connection():
        raise APIError("API connection validation failed")
    
    if not db_extractor.validate_database_connection():
        raise DatabaseError("Database connection validation failed")
    
    # Extract data
    with timer(f"Incremental extraction from {start_date} to {end_date}", logger):
        data = {
            'users': api_extractor.extract_users(start_date, end_date),
            'sessions': api_extractor.extract_sessions(start_date, end_date),
            'songs': db_extractor.extract_songs(),
            'extraction_metadata': {
                'start_date': start_date,
                'end_date': end_date,
                'extraction_timestamp': datetime.utcnow().isoformat(),
                'environment': settings.pipeline.environment.value
            }
        }
    
    # Log summary
    logger.info("Incremental extraction completed", extra={
        'users_count': len(data['users']),
        'sessions_count': len(data['sessions']),
        'songs_count': len(data['songs']),
        'start_date': start_date,
        'end_date': end_date
    })
    
    return data


if __name__ == "__main__":
    # Example usage
    import sys
    from datetime import datetime, timedelta
    
    # Get date range from command line or use defaults
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Extract data
    try:
        data = extract_incremental_data(start_date, end_date)
        print(f"Extraction completed successfully!")
        print(f"Users: {len(data['users'])}")
        print(f"Sessions: {len(data['sessions'])}")
        print(f"Songs: {len(data['songs'])}")
    except Exception as e:
        print(f"Extraction failed: {str(e)}")
        sys.exit(1) 