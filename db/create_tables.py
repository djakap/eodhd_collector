"""
Create QuestDB tables using Python
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.db_config import PG_CONNECTION_STRING
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables(schema_name: str = 'schemas.sql'):
    """Create tables in QuestDB from a schema file"""

    # Read schema file
    schema_file = os.path.join(os.path.dirname(__file__), schema_name)
    with open(schema_file, 'r') as f:
        schema_sql = f.read()

    # Strip '--' comments before splitting: a semicolon inside a comment would
    # otherwise be treated as a statement boundary and produce invalid SQL.
    body = '\n'.join(line.split('--')[0] for line in schema_sql.splitlines())

    # Split into individual statements
    statements = [s.strip() for s in body.split(';') if s.strip()]
    
    try:
        # Connect to QuestDB
        logger.info(f"Connecting to QuestDB...")
        conn = psycopg2.connect(PG_CONNECTION_STRING)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Execute each statement
        for i, statement in enumerate(statements, 1):
            # Skip empty statements and comments
            if statement.strip() and not statement.strip().startswith('--'):
                try:
                    logger.info(f"Executing statement {i}/{len(statements)}...")
                    cursor.execute(statement)
                    logger.info(f"✅ Statement {i} executed successfully")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.warning(f"⚠️  Statement {i} - Table/Index already exists, skipping")
                    else:
                        logger.error(f"❌ Statement {i} failed: {e}")
                        raise
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "="*70)
        logger.info("✅ All tables created successfully!")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create QuestDB tables from a schema file")
    parser.add_argument('--schema', default='schemas.sql',
                        help="Schema file in db/ (e.g. schemas_yfinance.sql)")
    args = parser.parse_args()

    create_tables(args.schema)
