"""
SQL generation for RisingWave CDC jobs.

This module is responsible for generating RisingWave SQL statements from a validated
job configuration. It uses Jinja2 templates to create the necessary SQL for:
1. Setting up the source connection
2. Creating the source
3. Creating the sink
4. Establishing the data flow
"""

from typing import Dict, Any
from sql_templates import (
    PG_CONNECTION_TEMPLATE,
    PG_SOURCE_TEMPLATE,
    ICEBERG_SINK_TEMPLATE,
    INSERT_TEMPLATE
)

def generate_sql(config: Dict[str, Any]) -> str:
    """
    Generate RisingWave SQL from the job configuration.
    
    This function takes a validated job configuration and generates the complete
    set of SQL statements needed to set up the CDC pipeline. The generated SQL
    will:
    1. Create a connection to the source PostgreSQL database
    2. Create a source that reads from the specified PostgreSQL table
    3. Create a sink that writes to the specified Iceberg table
    4. Set up a continuous query to copy data from source to sink
    
    Args:
        config: The validated job configuration dictionary
        
    Returns:
        A string containing the complete set of SQL statements, separated by newlines
    """
    # Extract source and sink configurations
    job = config['job']
    source = job['source']
    sink = job['sink']
    
    # Generate SQL statements in the correct order
    sql_statements = []
    
    # 1. Create PostgreSQL connection
    # This must be done first as it's required by the source
    sql_statements.append(PG_CONNECTION_TEMPLATE.render(source=source))
    
    # 2. Create PostgreSQL source
    # This uses the connection created above
    sql_statements.append(PG_SOURCE_TEMPLATE.render(source=source))
    
    # 3. Create Iceberg sink
    # This sets up the target for the data
    sql_statements.append(ICEBERG_SINK_TEMPLATE.render(source=source, sink=sink))
    
    # 4. Create insert statement
    # This establishes the continuous data flow
    sql_statements.append(INSERT_TEMPLATE.render(source=source, sink=sink))
    
    # Join all statements with newlines
    return '\n'.join(sql_statements) 