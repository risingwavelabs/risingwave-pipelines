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
RisingWave connector templates package.

This package contains SQL templates for various RisingWave connectors.
Each connector type has its own module with specific templates.
"""

# Import PostgreSQL templates and classes
# Import common templates, utilities, and base classes
from .common import (
    DROP_CONNECTION_TEMPLATE,
    DROP_SINK_TEMPLATE,
    DROP_SOURCE_TEMPLATE,
    DROP_TABLE_TEMPLATE,
    INSERT_TEMPLATE,
    BaseConnector,
    get_sink_name,
    get_source_name,
    get_table_name,
)

# Import Iceberg templates and classes
from .iceberg import ICEBERG_CONNECTION_TEMPLATE, ICEBERG_SINK_TEMPLATE, IcebergConnector
from .postgres import PG_SOURCE_TEMPLATE, PG_TABLE_TEMPLATE, PostgreSQLConnector

# Make all templates and classes available at package level
__all__ = [
    # Base class
    "BaseConnector",
    # PostgreSQL templates
    "PG_SOURCE_TEMPLATE",
    "PG_TABLE_TEMPLATE",
    # PostgreSQL connector class
    "PostgreSQLConnector",
    # Iceberg templates
    "ICEBERG_CONNECTION_TEMPLATE",
    "ICEBERG_SINK_TEMPLATE",
    # Iceberg connector class
    "IcebergConnector",
    # Common templates
    "INSERT_TEMPLATE",
    "DROP_CONNECTION_TEMPLATE",
    "DROP_SOURCE_TEMPLATE",
    "DROP_TABLE_TEMPLATE",
    "DROP_SINK_TEMPLATE",
    # Utility functions
    "get_table_name",
    "get_source_name",
    "get_sink_name",
]
