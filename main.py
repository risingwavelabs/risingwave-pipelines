#!/usr/bin/env python3
# Copyright 2025 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
import yaml

from generator import generate_sql
from parser import parse_yaml


def format_sql_statement(statement: str) -> str:
    """
    Format a single SQL statement for better readability.

    Args:
        statement: A single SQL statement

    Returns:
        Formatted SQL statement with proper indentation and line breaks
    """
    statement = statement.strip()
    if not statement:
        return ""

    # Add a header line with proper spacing
    formatted = "\n-- Statement --------------------------------------------------\n"

    # Basic formatting for common SQL keywords
    keywords = ["CREATE", "WITH", "FROM", "TABLE", "FORMAT", "ENCODE"]
    lines = []
    current_indent = 0

    for line in statement.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Adjust indentation based on content
        if any(line.startswith(keyword) for keyword in keywords):
            current_indent = 0
        elif line.startswith(("(", "{")):
            current_indent += 4
        elif line.startswith(("}", ")")):
            current_indent = max(0, current_indent - 4)

        # Add the line with proper indentation
        lines.append(" " * current_indent + line)

    formatted += "\n".join(lines)
    formatted += "\n"  # Single newline after statement
    return formatted


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
        print("\n🔌 Connecting to RisingWave...")
        conn = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        conn.autocommit = True
        print("✅ Connected successfully!")

        # Execute each SQL statement
        print("\n📝 Executing SQL statements:")
        with conn.cursor() as cur:
            for statement in sql.split(";"):
                if statement.strip():
                    # Display the formatted statement
                    print(format_sql_statement(statement))

                    # Execute and show result
                    print("⚡ Executing...")
                    cur.execute(statement)
                    print("✅ Success!\n")  # Add newline after success message

                    # If the statement returns results, display them
                    if cur.description:
                        rows = cur.fetchall()
                        if rows:
                            print("📊 Results:")
                            # Print column headers
                            headers = [desc[0] for desc in cur.description]
                            print("| " + " | ".join(headers) + " |")
                            print("|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|")
                            # Print rows
                            for row in rows:
                                print("| " + " | ".join(str(val) for val in row) + " |")
                            print()  # Add newline after results

        print("🎉 All SQL statements executed successfully!")
        conn.close()

    except psycopg2.Error as e:
        print(f"\n❌ Error connecting to RisingWave: {e}", file=sys.stderr)
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
    parser = argparse.ArgumentParser(description="RisingWave CDC CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Configure the 'run' command
    run_parser = subparsers.add_parser("run", help="Run a CDC job")
    run_parser.add_argument("-f", "--file", required=True, help="Path to job YAML file")
    run_parser.add_argument("--submit", action="store_true", help="Submit SQL to RisingWave")
    run_parser.add_argument("--host", help="RisingWave host")
    run_parser.add_argument("--port", type=int, help="RisingWave port")
    run_parser.add_argument("--database", help="RisingWave database")
    run_parser.add_argument("--user", help="RisingWave username")
    run_parser.add_argument("--password", help="RisingWave password")

    # Parse arguments
    args = parser.parse_args()

    # Validate command
    if args.command != "run":
        parser.print_help()
        sys.exit(1)

    # Parse and validate YAML
    try:
        config = parse_yaml(args.file)

        # Extract job config if wrapped, otherwise use config as-is
        if "job" in config:
            job_config = config["job"]
        else:
            job_config = config

    except (FileNotFoundError, yaml.YAMLError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Generate SQL
    sql = generate_sql(job_config)

    if args.submit:
        # Validate RisingWave connection parameters
        required_params = ["host", "port", "database", "user"]
        missing_params = [param for param in required_params if not getattr(args, param)]
        if missing_params:
            print(
                "Error: Missing required parameters for RisingWave connection: "
                f"{', '.join(missing_params)}",
                file=sys.stderr,
            )
            sys.exit(1)

        # Submit SQL to RisingWave
        submit_sql(sql, args.host, args.port, args.database, args.user, args.password)
    else:
        # Print SQL to stdout with formatting
        print("\n📝 Generated SQL statements:")
        for statement in sql.split(";"):
            if statement.strip():
                print(format_sql_statement(statement))


if __name__ == "__main__":
    main()
