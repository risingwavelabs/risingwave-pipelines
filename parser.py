"""
YAML parsing and validation for RisingWave CDC jobs.

This module handles:
1. Loading and parsing YAML configuration files
2. Validating the configuration structure and required fields
3. Ensuring all necessary connection details are provided
"""

import yaml
from typing import Dict, Any

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
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Job file not found: {file_path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Invalid YAML file: {e}")

def validate_job_config(config: Dict[str, Any]) -> None:
    """
    Validate the job configuration.
    
    This function performs a thorough validation of the job configuration:
    1. Checks for the presence of required top-level keys
    2. Validates source configuration (PostgreSQL connection details)
    3. Validates sink configuration (Iceberg connection details)
    4. Ensures all required fields are present and properly formatted
    
    Args:
        config: The job configuration dictionary
        
    Raises:
        ValueError: If the configuration is invalid or missing required fields
    """
    # Check for top-level 'job' key
    if 'job' not in config:
        raise ValueError("Missing 'job' key in configuration")
    
    job = config['job']
    
    # Validate source configuration
    if 'source' not in job:
        raise ValueError("Missing 'source' configuration")
    source = job['source']
    required_source_fields = ['type', 'hostname', 'port', 'username', 'password', 'database', 'table']
    for field in required_source_fields:
        if field not in source:
            raise ValueError(f"Missing required source field: {field}")
    
    # Validate sink configuration
    if 'sink' not in job:
        raise ValueError("Missing 'sink' configuration")
    sink = job['sink']
    required_sink_fields = ['type', 'catalog', 'table']
    for field in required_sink_fields:
        if field not in sink:
            raise ValueError(f"Missing required sink field: {field}")
    
    # Validate sink catalog configuration
    catalog = sink['catalog']
    required_catalog_fields = ['uri', 'warehouse']
    for field in required_catalog_fields:
        if field not in catalog:
            raise ValueError(f"Missing required catalog field: {field}") 