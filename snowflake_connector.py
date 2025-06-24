#!/usr/bin/env python3
"""
Snowflake Connector for Data Pipeline
Connects to Snowflake and executes SQL scripts to set up external tables
"""

import os
import logging
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """Handles Snowflake connections and SQL execution"""
    
    def __init__(self):
        """Initialize Snowflake connection parameters from environment variables"""
        self.connection_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
        
        # Validate required parameters
        missing_params = [key for key, value in self.connection_params.items() if not value]
        if missing_params:
            raise ValueError(f"Missing Snowflake configuration: {missing_params}")
        
        self.connection = None
        logger.info("Snowflake connector initialized")

    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            import snowflake.connector
            
            self.connection = snowflake.connector.connect(
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                account=self.connection_params['account'],
                warehouse=self.connection_params['warehouse'],
                database=self.connection_params['database'],
                schema=self.connection_params['schema']
            )
            
            logger.info("✅ Successfully connected to Snowflake")
            return True
            
        except ImportError:
            logger.error("❌ snowflake-connector-python not installed. Run: pip install snowflake-connector-python")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            return False

    def execute_sql_file(self, file_path: str, substitute_variables: bool = True) -> bool:
        """Execute SQL commands from a file"""
        if not self.connection:
            logger.error("No Snowflake connection established")
            return False
            
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            # Substitute environment variables if requested
            if substitute_variables:
                sql_content = self._substitute_env_variables(sql_content)
            
            # Split SQL content by semicolons and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            cursor = self.connection.cursor()
            
            for i, statement in enumerate(statements, 1):
                if statement.strip().startswith('--') or not statement.strip():
                    continue
                    
                try:
                    logger.info(f"Executing statement {i}/{len(statements)}")
                    cursor.execute(statement)
                    
                    # Fetch results if it's a SELECT statement
                    if statement.strip().upper().startswith('SELECT') or statement.strip().upper().startswith('SHOW'):
                        results = cursor.fetchall()
                        if results:
                            logger.info(f"Query returned {len(results)} rows")
                            # Log first few rows for debugging
                            for row in results[:3]:
                                logger.debug(f"  {row}")
                    
                except Exception as e:
                    logger.warning(f"Statement {i} failed: {e}")
                    continue
            
            cursor.close()
            logger.info(f"✅ Successfully executed SQL file: {file_path}")
            return True
            
        except FileNotFoundError:
            logger.error(f"❌ SQL file not found: {file_path}")
            return False
        except Exception as e:
            logger.error(f"❌ Error executing SQL file {file_path}: {e}")
            return False

    def execute_query(self, query: str) -> Optional[List[tuple]]:
        """Execute a single SQL query and return results"""
        if not self.connection:
            logger.error("No Snowflake connection established")
            return None
            
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"❌ Error executing query: {e}")
            return None

    def test_external_tables(self) -> Dict[str, Any]:
        """Test the external tables and return status"""
        test_results = {}
        
        tables_to_test = [
            ('ext_raw_users', 'SELECT COUNT(*) FROM ext_raw_users'),
            ('ext_user_analytics', 'SELECT COUNT(*) FROM ext_user_analytics'),
            ('ext_user_demographics', 'SELECT COUNT(*) FROM ext_user_demographics')
        ]
        
        for table_name, query in tables_to_test:
            try:
                results = self.execute_query(query)
                if results:
                    count = results[0][0]
                    test_results[table_name] = {
                        'status': 'success',
                        'record_count': count
                    }
                    logger.info(f"✅ {table_name}: {count} records")
                else:
                    test_results[table_name] = {
                        'status': 'error',
                        'error': 'No results returned'
                    }
            except Exception as e:
                test_results[table_name] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"❌ {table_name}: {e}")
        
        return test_results

    def _substitute_env_variables(self, sql_content: str) -> str:
        """Substitute environment variables in SQL content"""
        import re
        
        # Replace ${VAR_NAME} patterns with environment variable values
        def replace_env_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, f"${{{var_name}}}")  # Keep original if not found
        
        sql_content = re.sub(r'\$\{([^}]+)\}', replace_env_var, sql_content)
        return sql_content

    def close(self):
        """Close the Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed")


def setup_snowflake_tables():
    """Main function to set up all Snowflake tables and stages"""
    connector = SnowflakeConnector()
    
    try:
        # Connect to Snowflake
        if not connector.connect():
            return False
        
        # Execute SQL files in order
        sql_files = [
            'snowflake/01_setup_stages.sql',
            'snowflake/02_create_file_formats.sql',
            'snowflake/03_create_external_tables.sql'
        ]
        
        for sql_file in sql_files:
            if not connector.execute_sql_file(sql_file):
                logger.error(f"Failed to execute {sql_file}")
                return False
        
        # Test the external tables
        logger.info("Testing external tables...")
        test_results = connector.test_external_tables()
        
        # Print summary
        print("\n" + "="*60)
        print("🎉 SNOWFLAKE SETUP COMPLETED!")
        print("="*60)
        
        for table_name, result in test_results.items():
            if result['status'] == 'success':
                print(f"✅ {table_name}: {result['record_count']} records")
            else:
                print(f"❌ {table_name}: {result['error']}")
        
        print("\nNext steps:")
        print("1. Run your data pipeline: python realtime_pipeline.py")
        print("2. Execute sample queries: snowflake/04_sample_queries.sql")
        print("3. Check Snowflake UI for data visualization")
        print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return False
    finally:
        connector.close()


def run_sample_queries():
    """Execute sample queries to demonstrate the data"""
    connector = SnowflakeConnector()
    
    try:
        if not connector.connect():
            return False
        
        logger.info("Running sample queries...")
        
        # Simple test queries
        test_queries = [
            ("Total Users", "SELECT COUNT(*) as total_users FROM ext_raw_users"),
            ("Gender Distribution", """
                SELECT 
                    metric_name,
                    metric_value as count,
                    percentage
                FROM ext_user_analytics 
                WHERE metric_type = 'gender_distribution'
                ORDER BY metric_value DESC
            """),
            ("Top Countries", """
                SELECT 
                    demographic_value as country,
                    user_count
                FROM ext_user_demographics
                WHERE demographic_type = 'country_distribution'
                ORDER BY user_count DESC
                LIMIT 5
            """)
        ]
        
        for query_name, query in test_queries:
            print(f"\n--- {query_name} ---")
            results = connector.execute_query(query)
            if results:
                for row in results:
                    print(f"  {row}")
            else:
                print("  No results")
        
        return True
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return False
    finally:
        connector.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        run_sample_queries()
    else:
        setup_snowflake_tables() 