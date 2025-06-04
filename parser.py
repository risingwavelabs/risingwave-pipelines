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
YAML parsing and validation for RisingWave CDC jobs.

This module handles:
1. Loading and parsing YAML configuration files
2. Validating the configuration structure and required fields
3. Ensuring all necessary connection details are provided
"""

from typing import Any, Dict

import yaml


def parse_yaml(file_path: str) -> Dict[str, Any]:
    """
    Parse a YAML file and return its contents as a dictionary.

    This function reads a YAML file and converts it into a Python dictionary.
    It handles common errors like missing files and invalid YAML syntax.

    Args:
        file_path: Path to the YAML file

    Returns:
        Dictionary containing the parsed YAML contents

    Raises:
        FileNotFoundError: If the file doesn't exist
        yaml.YAMLError: If the YAML is invalid
    """
    try:
        with open(file_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError as err:
        raise FileNotFoundError(f"Job file not found: {file_path}") from err
    except yaml.YAMLError as err:
        raise yaml.YAMLError(f"Invalid YAML file: {err}") from err
