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
Common templates and utilities shared across connectors.

This module contains templates and utility functions that can be used
by multiple connector types.
"""

from abc import ABC, abstractmethod

from jinja2 import Template


class BaseConnector(ABC):
    """
    Base class for all RisingWave connectors.

    This abstract class provides common functionality and defines the interface
    that all connector classes must implement.
    """

    def __init__(self, name: str):
        """
        Initialize the connector with a name.

        Args:
            name (str): Name identifier for this connector instance
        """
        self.name = name

    @abstractmethod
    def get_connector_type(self) -> str:
        """
        Get the connector type string used in RisingWave SQL.

        Returns:
            str: The connector type (e.g., 'postgres-cdc', 'iceberg')
        """
        pass

    def supports_connection(self) -> bool:
        """
        Check if this connector supports CREATE CONNECTION statements.

        Returns:
            bool: True if the connector supports connections, False otherwise
        """
        return False  # Default to False, override in connectors that support it

    def render_template(self, template: Template, **kwargs) -> str:
        """
        Render a Jinja2 template with the provided context.

        Args:
            template (Template): The Jinja2 template to render
            **kwargs: Template context variables

        Returns:
            str: The rendered template
        """
        return template.render(**kwargs)

    def get_table_name(self, qualified_name: str) -> str:
        """
        Extract the table name from a qualified name.

        Args:
            qualified_name (str): Qualified table name like 'schema.table'

        Returns:
            str: Just the table name part
        """
        return qualified_name.split(".")[-1]


# Template for the insert statement (if needed for manual data flow)
# Note: With CDC sources and sinks, data flows automatically, so this may not be needed
INSERT_TEMPLATE = Template("""
INSERT INTO {{ sink.table.split('.')[-1] }}
SELECT * FROM {{ source.table.split('.')[-1] }};
""")

# Template for dropping a connection
DROP_CONNECTION_TEMPLATE = Template("""
DROP CONNECTION IF EXISTS {{ connection_name }};
""")

# Template for dropping a source
DROP_SOURCE_TEMPLATE = Template("""
DROP SOURCE IF EXISTS {{ source_name }};
""")

# Template for dropping a table
DROP_TABLE_TEMPLATE = Template("""
DROP TABLE IF EXISTS {{ table_name }};
""")

# Template for dropping a sink
DROP_SINK_TEMPLATE = Template("""
DROP SINK IF EXISTS {{ sink_name }};
""")


# Helper function to extract table name from a potentially qualified table name
def get_table_name(table_qualified_name):
    """
    Extract the table name from a qualified name like 'schema.table' or 'database.schema.table'.
    Returns just the table name part.

    Args:
        table_qualified_name (str): Qualified table name

    Returns:
        str: Just the table name
    """
    return table_qualified_name.split(".")[-1]


# Helper function to get source name with suffix
def get_source_name(table_name, suffix="_source"):
    """
    Generate a source name from a table name.

    Args:
        table_name (str): Table name
        suffix (str): Suffix to append (default: "_source")

    Returns:
        str: Source name
    """
    return f"{get_table_name(table_name)}{suffix}"


# Helper function to get sink name with suffix
def get_sink_name(table_name, suffix="_sink"):
    """
    Generate a sink name from a table name.

    Args:
        table_name (str): Table name
        suffix (str): Suffix to append (default: "_sink")

    Returns:
        str: Sink name
    """
    return f"{get_table_name(table_name)}{suffix}"
