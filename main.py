#!/usr/bin/env python3

"""
RisingWave CDC CLI tool.

This module provides the command-line interface for the RisingWave CDC tool.
It handles:
1. Command-line argument parsing
2. Job configuration loading and validation
3. SQL generation
4. Optional SQL submission to RisingWave
"""

import argparse
import sys
import psycopg2
from parser import parse_yaml, validate_job_config
from generator import generate_sql

def submit_sql(sql: str, host: str, port: int, database: str, user: str, password: str) -> None:
    """
    Submit SQL statements to RisingWave.
    
    This function executes a series of SQL statements against a RisingWave instance.
    It:
    1. Establishes a connection to RisingWave
    2. Executes each SQL statement in sequence
    3. Handles any database errors that occur
    
    Args:
        sql: SQL statements to execute (semicolon-separated)
        host: RisingWave host
        port: RisingWave port
        database: Database name
        user: Username
        password: Password
        
    Raises:
        SystemExit: If there's an error connecting to or executing SQL on RisingWave
    """
    try:
        # Connect to RisingWave
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        conn.autocommit = True
        
        # Execute each SQL statement
        with conn.cursor() as cur:
            for statement in sql.split(';'):
                if statement.strip():
                    cur.execute(statement)
        conn.close()
    except psycopg2.Error as e:
        print(f"Error connecting to RisingWave: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    """
    Main entry point for the CLI tool.
    
    This function:
    1. Sets up command-line argument parsing
    2. Processes the job configuration
    3. Generates SQL statements
    4. Optionally submits the SQL to RisingWave
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='RisingWave CDC CLI')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Configure the 'run' command
    run_parser = subparsers.add_parser('run', help='Run a CDC job')
    run_parser.add_argument('-f', '--file', required=True, help='Path to job YAML file')
    run_parser.add_argument('--submit', action='store_true', help='Submit SQL to RisingWave')
    run_parser.add_argument('--host', help='RisingWave host')
    run_parser.add_argument('--port', type=int, help='RisingWave port')
    run_parser.add_argument('--database', help='RisingWave database')
    run_parser.add_argument('--user', help='RisingWave username')
    run_parser.add_argument('--password', help='RisingWave password')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate command
    if args.command != 'run':
        parser.print_help()
        sys.exit(1)
    
    # Parse and validate YAML
    try:
        config = parse_yaml(args.file)
        validate_job_config(config)
    except (FileNotFoundError, yaml.YAMLError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Generate SQL
    sql = generate_sql(config)
    
    if args.submit:
        # Validate RisingWave connection parameters
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [param for param in required_params if not getattr(args, param)]
        if missing_params:
            print(f"Error: Missing required parameters for RisingWave connection: {', '.join(missing_params)}", file=sys.stderr)
            sys.exit(1)
        
        # Submit SQL to RisingWave
        submit_sql(sql, args.host, args.port, args.database, args.user, args.password)
    else:
        # Print SQL to stdout
        print(sql)

if __name__ == '__main__':
    main() 