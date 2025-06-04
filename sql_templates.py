"""
SQL templates for RisingWave CDC jobs.

This module contains Jinja2 templates for generating RisingWave SQL statements.
Each template is designed to handle a specific part of the CDC pipeline:
1. Creating a PostgreSQL connection
2. Creating a PostgreSQL source
3. Creating an Iceberg sink
4. Setting up the data flow
"""

from jinja2 import Template

# Template for creating a PostgreSQL source connection
# This template generates the SQL to establish a connection to the source PostgreSQL database
# The connection will be used by the source to read data
PG_CONNECTION_TEMPLATE = Template("""
CREATE CONNECTION pg_connection
WITH (
    type = 'postgres',
    hostname = '{{ source.hostname }}',
    port = {{ source.port }},
    username = '{{ source.username }}',
    password = '{{ source.password }}',
    database = '{{ source.database }}'
);
""")

# Template for creating a PostgreSQL source
# This template generates the SQL to create a source that reads from the PostgreSQL table
# It uses the connection created above and specifies which table to read from
PG_SOURCE_TEMPLATE = Template("""
CREATE SOURCE {{ source.table.split('.')[-1] }}
FROM POSTGRES CONNECTION pg_connection
(TABLE '{{ source.table }}');
""")

# Template for creating an Iceberg sink
# This template generates the SQL to create a sink that writes to an Iceberg table
# It specifies the catalog connection details and target table
ICEBERG_SINK_TEMPLATE = Template("""
CREATE SINK {{ sink.table.split('.')[-1] }}
FROM {{ source.table.split('.')[-1] }}
INTO ICEBERG
WITH (
    catalog = '{{ sink.catalog.uri }}',
    warehouse = '{{ sink.catalog.warehouse }}',
    table = '{{ sink.table }}'
);
""")

# Template for the insert statement
# This template generates the SQL to set up the data flow from source to sink
# It creates a continuous query that copies all data from source to sink
INSERT_TEMPLATE = Template("""
INSERT INTO {{ sink.table.split('.')[-1] }}
SELECT * FROM {{ source.table.split('.')[-1] }};
""") 