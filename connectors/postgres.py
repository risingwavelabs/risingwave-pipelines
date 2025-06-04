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
PostgreSQL CDC connector templates for RisingWave.

This module contains Jinja2 templates for PostgreSQL CDC operations:
1. Creating a PostgreSQL CDC source
2. Creating a table from the PostgreSQL CDC source
"""

from typing import Any, Dict

from jinja2 import Template

from .common import BaseConnector

# Template for creating a PostgreSQL CDC source
# This template generates the SQL to create a CDC source that reads from PostgreSQL
# It uses the postgres-cdc connector with connection parameters
PG_SOURCE_TEMPLATE = Template("""
CREATE SOURCE postgres_{{ source.database.name }}_source
WITH (
    connector = 'postgres-cdc',
    hostname = '{{ source.hostname }}',
    port = '{{ source.port }}',
    username = '{{ source.username }}',
    password = '{{ source.password }}',
    database.name = '{{ source.database.name }}'
    {%- if source.get('slot', {}).get('name') %},
    slot.name = '{{ source.slot.name }}'
    {%- endif %}
    {%- if source.get('publication', {}).get('name') %},
    publication.name = '{{ source.publication.name }}'
    {%- endif %}
    {%- if source.get('publication', {}).get('create', {}).get('enable') %},
    publication.create.enable = '{{ source.publication.create.enable }}'
    {%- endif %}
    {%- if source.get('schema', {}).get('name') %},
    schema.name = '{{ source.schema.name }}'
    {%- endif %}
)
FORMAT PLAIN ENCODE JSON;
""")

# Template for creating a table from the PostgreSQL CDC source
# This template creates a table that consumes from the CDC source
PG_TABLE_TEMPLATE = Template("""
CREATE TABLE {{ route.source_table.split('.')[-1] }} (*)
FROM postgres_{{ source.database.name }}_source
TABLE '{{ route.source_table }}';
""")


class PostgreSQLConnector(BaseConnector):
    """
    PostgreSQL CDC connector for RisingWave source operations.

    This class provides methods to generate SQL statements for:
    1. Creating PostgreSQL CDC sources
    2. Creating tables from CDC sources
    """

    def __init__(self, name: str = "postgres_connector"):
        """
        Initialize the PostgreSQL connector.

        Args:
            name (str): Name identifier for this connector instance
        """
        super().__init__(name)

    def get_connector_type(self) -> str:
        """
        Get the connector type for PostgreSQL.

        Returns:
            str: The connector type 'postgres'
        """
        return "postgres"

    def supports_connection(self) -> bool:
        """
        PostgreSQL CDC does not support CREATE CONNECTION in RisingWave.

        Returns:
            bool: False, PostgreSQL CDC connects directly
        """
        return False

    def create_source(self, source_config: Dict[str, Any]) -> str:
        """
        Generate SQL to create a PostgreSQL CDC source.

        Args:
            source_config (Dict[str, Any]): Source configuration containing connection details

        Returns:
            str: SQL statement to create the PostgreSQL CDC source
        """
        return self.render_template(PG_SOURCE_TEMPLATE, source=source_config)

    def create_table(self, source_config: Dict[str, Any], route: Dict[str, Any]) -> str:
        """
        Generate SQL to create a table from a PostgreSQL CDC source.

        Args:
            source_config (Dict[str, Any]): Source configuration
            route (Dict[str, Any]): Route configuration containing source_table and sink_table

        Returns:
            str: SQL statement to create the table
        """
        return self.render_template(PG_TABLE_TEMPLATE, source=source_config, route=route)

    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate PostgreSQL source configuration.

        Args:
            config (Dict[str, Any]): Configuration to validate

        Raises:
            ValueError: If required fields are missing or invalid fields are present
        """
        required_fields = ["connector", "hostname", "port", "username", "password", "database"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required PostgreSQL source field: {field}")

        # Validate database configuration
        if isinstance(config["database"], dict):
            if "name" not in config["database"]:
                raise ValueError("Database configuration must have a 'name' field")
        else:
            # Convert string database name to dict format
            config["database"] = {"name": config["database"]}

        # Validate optional nested structures if present
        if "slot" in config:
            if not isinstance(config["slot"], dict):
                raise ValueError("Slot configuration must be a dictionary")
            if "name" not in config["slot"]:
                raise ValueError("Missing required slot.name field when slot is specified")

        if "publication" in config:
            if not isinstance(config["publication"], dict):
                raise ValueError("Publication configuration must be a dictionary")
            if "name" in config["publication"] and not isinstance(
                config["publication"]["name"], str
            ):
                raise ValueError("Publication name must be a string")
            if "create" in config["publication"]:
                if not isinstance(config["publication"]["create"], dict):
                    raise ValueError("Publication create configuration must be a dictionary")
                if "enable" not in config["publication"]["create"]:
                    raise ValueError(
                        "Missing required publication.create.enable field "
                        "when publication.create is specified"
                    )

        if "schema" in config:
            if not isinstance(config["schema"], dict):
                raise ValueError("Schema configuration must be a dictionary")
            if "name" not in config["schema"]:
                raise ValueError("Missing required schema.name field when schema is specified")

        # Validate data types
        if not isinstance(config["port"], (int, str)):
            raise ValueError("Port must be an integer or string")

        # Validate table format if present (should have at least schema.table)
        if "table" in config and "." not in config["table"]:
            raise ValueError("Table name should be in format 'schema.table'")
