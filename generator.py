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
SQL generation for RisingWave Pipeline jobs.

This module is responsible for generating RisingWave SQL statements from a validated
job configuration. It uses connector classes to create the necessary SQL for:
1. Setting up the source connection
2. Creating the source
3. Creating the sink
4. Establishing the data flow
"""

from typing import Any, Dict

from connectors import BaseConnector, IcebergConnector, PostgreSQLConnector

# Registry mapping connector types to their classes
CONNECTOR_REGISTRY = {
    "postgres": PostgreSQLConnector,
    "iceberg": IcebergConnector,
}


def get_connector_instance(connector_type: str, name: str) -> BaseConnector:
    """
    Create a connector instance based on the connector type.

    Args:
        connector_type: The type of connector (e.g., 'postgres', 'iceberg')
        name: Name identifier for the connector instance

    Returns:
        BaseConnector: An instance of the appropriate connector class

    Raises:
        ValueError: If the connector type is not supported
    """
    if connector_type not in CONNECTOR_REGISTRY:
        supported_types = ", ".join(CONNECTOR_REGISTRY.keys())
        raise ValueError(
            f"Unsupported connector type: {connector_type}. Supported types: {supported_types}"
        )

    connector_class = CONNECTOR_REGISTRY[connector_type]
    return connector_class(name)


def validate_route(route: Dict[str, str]) -> None:
    """
    Validate a single route configuration.

    Args:
        route: A dictionary containing source_table and sink_table

    Raises:
        ValueError: If required fields are missing
    """
    if "source_table" not in route:
        raise ValueError("Route must contain 'source_table'")
    if "sink_table" not in route:
        raise ValueError("Route must contain 'sink_table'")


def generate_sql(config: Dict[str, Any]) -> str:
    """
    Generate RisingWave SQL using connector classes.

    This function takes a validated job configuration and generates the complete
    set of SQL statements needed to set up the CDC pipeline. The generated SQL
    will:
    1. Create connections (only for connectors that support them)
    2. Create source (based on source type)
    3. Create a table from the source
    4. Create sink (based on sink type)

    Args:
        config: The validated job configuration dictionary

    Returns:
        A string containing the complete set of SQL statements, separated by newlines
    """
    # Extract source, sink, and route configurations
    source = config["source"]
    sink = config["sink"]
    routes = config.get("route", [])

    # Validate that connector fields exist
    if "connector" not in source:
        raise ValueError("Source configuration must include a 'connector' field")
    if "connector" not in sink:
        raise ValueError("Sink configuration must include a 'connector' field")

    # Create connector instances based on connector type
    source_connector = get_connector_instance(
        source["connector"], f"pipeline_{source['connector']}"
    )
    sink_connector = get_connector_instance(sink["connector"], f"pipeline_{sink['connector']}")

    # Validate configurations
    source_connector.validate_config(source)
    sink_connector.validate_config(sink)

    # Validate route configuration
    if not routes:
        raise ValueError("Route configuration is required")

    # Validate routes
    for route in routes:
        validate_route(route)

    # Generate SQL statements in the correct order
    sql_statements = []

    # 1. Create source connection (only if supported)
    if source_connector.supports_connection():
        connection_sql = source_connector.create_connection(
            source, f"{source['connector']}_connection"
        )
        sql_statements.append(connection_sql)

    # 2. Create source for this route
    source_sql = source_connector.create_source(source)
    sql_statements.append(source_sql)

    # Process each route
    for route in routes:
        # 3. Create table from source (if source connector supports it)
        if hasattr(source_connector, "create_table"):
            table_sql = source_connector.create_table(source, route)
            sql_statements.append(table_sql)

    # 4. Create sink connection (only if supported)
    if sink_connector.supports_connection():
        sink_connection_sql = sink_connector.create_connection(
            sink, f"{sink['connector']}_connection"
        )
        sql_statements.append(sink_connection_sql)

    # 5. Create sinks for each route
    for route in routes:
        if sink_connector.supports_connection():
            # Create sink with connection
            sink_with_conn = sink.copy()
            sink_with_conn["connection_name"] = f"{sink['connector']}_connection"
            sink_sql = sink_connector.create_sink(source, sink_with_conn, route)
        else:
            # Create sink without connection (direct configuration)
            sink_sql = sink_connector.create_sink(source, sink, route)

        sql_statements.append(sink_sql)

    # Join all statements with newlines
    return "\n".join(sql_statements)
