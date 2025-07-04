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
Iceberg connector templates for RisingWave.

This module contains Jinja2 templates for Iceberg operations:
1. Creating an Iceberg connection
2. Creating an Iceberg sink
"""

from typing import Any, Dict, Optional

from jinja2 import Template

from .common import BaseConnector

# Template for creating an Iceberg connection
# This template generates the SQL to establish a connection to the Iceberg catalog
# The connection will be used by the sink to write data
ICEBERG_CONNECTION_TEMPLATE = Template("""
CREATE CONNECTION {{ connection_name }}
WITH (
    type = 'iceberg'
    {%- for prop in properties %},
    {{ prop }}
    {%- endfor %}
);
""")

# Template for creating an Iceberg sink
# This template generates the SQL to create a sink that writes to an Iceberg table
# It uses the Iceberg connection created above
ICEBERG_SINK_TEMPLATE = Template("""
CREATE SINK {{ sink_name }}_sink
FROM {{ from_table }}
WITH (
    connector = 'iceberg',
    database.name = '{{ database_name }}',
    table.name = '{{ table_name }}'
    {%- if connection_name %},
    connection = {{ connection_name }}
    {%- endif %}
    {%- for prop in properties %},
    {{ prop }}
    {%- endfor %}
);
""")


class IcebergConnector(BaseConnector):
    """
    Iceberg connector for RisingWave sink operations.

    This class provides methods to generate SQL statements for:
    1. Creating Iceberg connections
    2. Creating Iceberg sinks (using connections)
    """

    def __init__(self, name: str = "iceberg_connector"):
        """
        Initialize the Iceberg connector.

        Args:
            name (str): Name identifier for this connector instance
        """
        super().__init__(name)
        self.valid_connection_props = {"catalog", "s3", "gcs", "azblob", "warehouse"}

    def get_connector_type(self) -> str:
        """
        Get the connector type for Iceberg.

        Returns:
            str: The connector type 'iceberg'
        """
        return "iceberg"

    def supports_connection(self) -> bool:
        """
        Iceberg supports CREATE CONNECTION in RisingWave.

        Returns:
            bool: True, Iceberg supports connections
        """
        return True

    def create_connection(self, sink_config: Dict[str, Any], connection_name: str) -> str:
        """
        Generate SQL to create an Iceberg connection.

        Args:
            sink_config (Dict[str, Any]): Sink configuration containing catalog details
            connection_name (str): Name for the connection

        Returns:
            str: SQL statement to create the Iceberg connection
        """

        properties = []

        def quote_if_string(v):
            if isinstance(v, str):
                return f"'{v}'"
            return v

        def flatten_dict(d: Dict[str, Any], parent_key: str = ""):
            for k, v in d.items():
                new_key = f"{parent_key}.{k}" if parent_key else k
                if isinstance(v, dict):
                    flatten_dict(v, new_key)
                else:
                    properties.append(f"{new_key} = {quote_if_string(v)}")

        # Unfold all parameters from sink config, only processing valid connection properties
        for key, value in sink_config.items():
            if key not in self.valid_connection_props:
                continue
            if isinstance(value, dict):
                flatten_dict(value, key)
            else:
                properties.append(f"{key} = {quote_if_string(value)}")

        return self.render_template(
            ICEBERG_CONNECTION_TEMPLATE, properties=properties, connection_name=connection_name
        )

    def create_sink(
        self,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        route: Dict[str, Any],
        connection_name: Optional[str] = None,
    ) -> str:
        """
        Generate SQL to create an Iceberg sink (using a connection).

        Args:
            source_config (Dict[str, Any]): Source configuration
            sink_config (Dict[str, Any]): Sink configuration
            route (Dict[str, Any]): Route configuration containing source_table,
                sink_table and primary key
            connection_name (Optional[str], optional): Name of the connection to use.
                Defaults to None.

        Returns:
            str: SQL statement to create the Iceberg sink
        """

        properties = []
        flattened_props = {}

        def quote_if_string(v):
            if isinstance(v, str):
                return f"'{v}'"
            if isinstance(v, bool):
                return f"'{str(v).lower()}'"
            return str(v)

        def flatten_dict_to_map(
            d: Dict[str, Any], target_map: Dict[str, Any], parent_key: str = ""
        ):
            for k, v in d.items():
                new_key = f"{parent_key}.{k}" if parent_key else k
                if isinstance(v, dict):
                    flatten_dict_to_map(v, target_map, new_key)
                else:
                    target_map[new_key] = v

        def process_config(config, target_map):
            for key, value in config.items():
                if isinstance(value, dict):
                    flatten_dict_to_map(value, target_map, key)
                else:
                    target_map[key] = value

        # Unfold all parameters from sink and route configs
        # route config overwrites sink config
        process_config(sink_config, flattened_props)
        process_config(route, flattened_props)

        # Convert flattened map to list of properties for template
        for key, value in flattened_props.items():
            # These properties are handled by the template directly, are not sink properties,
            # or are connection properties
            if (
                key
                in [
                    "connector",
                    "database.name",
                    "table.name",
                    "source_table",
                    "sink_table",
                    "connection",
                ]
                or key.split(".")[0] in self.valid_connection_props
            ):
                continue
            properties.append(f"{key} = {quote_if_string(value)}")

        # Extract required fields for the template
        sink_name = route["sink_table"].split(".")[-1]
        from_table = route["source_table"].split(".")[-1]
        database_name = ".".join(route["sink_table"].split(".")[:-1])
        table_name = route["sink_table"].split(".")[-1]

        return self.render_template(
            ICEBERG_SINK_TEMPLATE,
            properties=properties,
            sink_name=sink_name,
            from_table=from_table,
            database_name=database_name,
            table_name=table_name,
            connection_name=connection_name,
        )

    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate Iceberg sink configuration.

        Args:
            config (Dict[str, Any]): Configuration to validate

        Raises:
            ValueError: If required fields are missing or invalid fields are present
        """
        required_fields = ["connector", "catalog"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required Iceberg sink field: {field}")

        # Check for forbidden fields - database and table should be derived from source
        forbidden_fields = ["database", "database.name"]
        for field in forbidden_fields:
            if field in config:
                raise ValueError(
                    f"Field '{field}' should not be specified in Iceberg sink config. "
                    f"Database and table names are derived from the route configuration."
                )

        # Validate catalog configuration
        catalog = config["catalog"]
        if not isinstance(catalog, dict):
            raise ValueError("Catalog configuration must be a dictionary")

        # Check for at least one storage configuration
        storage_types = ["rest", "jdbc"]
        has_storage_config = any(storage_type in catalog for storage_type in storage_types)
        root_storage_types = ["s3", "gcs", "azblob"]
        has_root_storage = any(storage_type in config for storage_type in root_storage_types)

        if not has_storage_config and not has_root_storage and "warehouse" not in config:
            raise ValueError(
                "Missing required configuration: either warehouse path, "
                "storage configuration (s3, gcs, azblob), or catalog configuration (rest, jdbc)"
            )

        # Validate warehouse configuration if present
        if "warehouse" in config:
            if not isinstance(config["warehouse"], dict):
                raise ValueError("Warehouse configuration must be a dictionary")
            if "path" not in config["warehouse"]:
                raise ValueError("Warehouse configuration must have a 'path' field")

        # Validate storage configurations if present
        if "s3" in config:
            s3_config = config["s3"]
            if not isinstance(s3_config, dict):
                raise ValueError("S3 configuration must be a dictionary")
            required_s3_fields = ["endpoint", "region", "access", "secret"]
            for field in required_s3_fields:
                if field not in s3_config:
                    raise ValueError(f"Missing required S3 field: {field}")

        if "gcs" in config:
            gcs_config = config["gcs"]
            if not isinstance(gcs_config, dict):
                raise ValueError("GCS configuration must be a dictionary")
            if "credential" not in gcs_config:
                raise ValueError("Missing required GCS field: credential")

        if "azblob" in config:
            azblob_config = config["azblob"]
            if not isinstance(azblob_config, dict):
                raise ValueError("Azure Blob configuration must be a dictionary")
            required_azblob_fields = ["account_name", "account_key", "endpoint_url"]
            for field in required_azblob_fields:
                if field not in azblob_config:
                    raise ValueError(f"Missing required Azure Blob field: {field}")

        if "rest" in catalog:
            rest_config = catalog["rest"]
            if not isinstance(rest_config, dict):
                raise ValueError("REST configuration must be a dictionary")
            required_rest_fields = ["credential", "token"]
            for field in required_rest_fields:
                if field not in rest_config:
                    raise ValueError(f"Missing required REST field: {field}")

        if "jdbc" in catalog:
            jdbc_config = catalog["jdbc"]
            if not isinstance(jdbc_config, dict):
                raise ValueError("JDBC configuration must be a dictionary")
            required_jdbc_fields = ["user", "password"]
            for field in required_jdbc_fields:
                if field not in jdbc_config:
                    raise ValueError(f"Missing required JDBC field: {field}")

        # Validate sink-specific options if present
        if "commit_checkpoint_interval" in config and not isinstance(
            config["commit_checkpoint_interval"], (int, float)
        ):
            raise ValueError("commit_checkpoint_interval must be a number")

        if "commit_retry_num" in config and not isinstance(config["commit_retry_num"], int):
            raise ValueError("commit_retry_num must be an integer")

        if "create_table_if_not_exists" in config and not isinstance(
            config["create_table_if_not_exists"], str
        ):
            raise ValueError("create_table_if_not_exists must be a string")

    def get_catalog_type(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Determine the catalog type based on configuration.

        Args:
            config (Dict[str, Any]): Full sink configuration

        Returns:
            Optional[str]: The catalog type or None if not determinable
        """
        catalog_config = config.get("catalog", {})
        if "s3" in config:
            return "hive"  # or 'hadoop' depending on setup
        elif "rest" in catalog_config:
            return "rest"
        elif "jdbc" in catalog_config:
            return "jdbc"
        elif "warehouse" in config:
            return "hadoop"  # default for warehouse-based configs

        return None
