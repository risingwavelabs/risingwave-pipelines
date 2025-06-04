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

#!/usr/bin/env python3
"""
Data-driven test suite for the RisingWave CDC generator module.

This test suite uses external YAML configuration files and expected
output files to test the generator functionality in a data-driven manner.

Set GENERATE_EXPECTED=1 environment variable to generate/update expected output files
instead of comparing against them.
"""

import os
import re
import sys
import unittest
from pathlib import Path

import yaml

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generator import generate_sql


class TestDataDriven(unittest.TestCase):
    """Data-driven tests using YAML configurations and expected outputs."""

    def setUp(self):
        """Set up test paths."""
        self.test_dir = Path(__file__).parent
        self.config_dir = self.test_dir / "data" / "configs"
        self.expected_dir = self.test_dir / "data" / "expected"
        self.examples_dir = Path(__file__).parent.parent / "examples"

        # Check if we should generate expected results instead of comparing
        self.generate_expected = os.environ.get("GENERATE_EXPECTED", "0").lower() in (
            "1",
            "true",
            "yes",
        )

        # Ensure expected directory exists when generating
        if self.generate_expected:
            self.expected_dir.mkdir(parents=True, exist_ok=True)

    def load_config(self, config_name, from_examples=False):
        """Load a YAML configuration file.

        Args:
            config_name: Name of the configuration file without extension
            from_examples: If True, load from examples directory instead of test configs
        """
        if from_examples:
            config_path = self.examples_dir / f"{config_name}.yaml"
        else:
            config_path = self.config_dir / f"{config_name}.yaml"

        with open(config_path) as f:
            config = yaml.safe_load(f)

        # Remove metadata fields that aren't part of the configuration
        config.pop("name", None)
        config.pop("description", None)

        return config

    def load_expected_sql(self, expected_name):
        """Load expected SQL output."""
        expected_path = self.expected_dir / f"{expected_name}.sql"
        with open(expected_path) as f:
            return f.read().strip()

    def load_expected_error(self, expected_name):
        """Load expected error message."""
        expected_path = self.expected_dir / f"{expected_name}.error"
        with open(expected_path) as f:
            return f.read().strip()

    def save_expected_sql(self, expected_name, sql_content):
        """Save expected SQL output to file."""
        expected_path = self.expected_dir / f"{expected_name}.sql"
        with open(expected_path, "w") as f:
            f.write(sql_content)
        print(f"Generated expected SQL: {expected_path}")

    def save_expected_error(self, expected_name, error_content):
        """Save expected error message to file."""
        expected_path = self.expected_dir / f"{expected_name}.error"
        with open(expected_path, "w") as f:
            f.write(error_content)
        print(f"Generated expected error: {expected_path}")

    def normalize_sql(self, sql):
        """Normalize SQL for comparison by removing extra whitespace and
        standardizing formatting."""
        if not sql:
            return ""

        # Remove comments and extra whitespace
        lines = []
        for line in sql.split("\n"):
            line = line.strip()
            if line and not line.startswith("--"):
                lines.append(line)

        # Join lines and normalize whitespace
        normalized = " ".join(lines)
        normalized = re.sub(r"\s+", " ", normalized)

        # Split by semicolon and normalize each statement
        statements = []
        for stmt in normalized.split(";"):
            stmt = stmt.strip()
            if stmt:
                # Normalize specific SQL patterns
                stmt = re.sub(r"\s*=\s*", " = ", stmt)  # Normalize = spacing
                stmt = re.sub(r"\s*,\s*", ", ", stmt)  # Normalize comma spacing
                stmt = re.sub(r"\s*\(\s*", " (", stmt)  # Normalize opening parentheses
                stmt = re.sub(r"\s*\)\s*", ") ", stmt)  # Normalize closing parentheses

                # Sort parameters within parentheses for order independence
                def sort_params(match):
                    params = match.group(1).split(",")
                    sorted_params = sorted([p.strip() for p in params if p.strip()])
                    return "(" + ", ".join(sorted_params) + ")"

                # Find WITH (...) blocks and sort their parameters
                stmt = re.sub(r"WITH\s*\((.*?)\)", lambda m: "WITH " + sort_params(m), stmt)

                statements.append(stmt)

        return ";\n".join(statements) + ";" if statements else ""

    def compare_sql(self, actual_sql, expected_sql, test_name):
        """Compare actual and expected SQL with detailed error reporting."""
        actual_normalized = self.normalize_sql(actual_sql)
        expected_normalized = self.normalize_sql(expected_sql)

        if actual_normalized == expected_normalized:
            return True

        # If they don't match, provide detailed comparison
        actual_lines = actual_normalized.split("\n")
        expected_lines = expected_normalized.split("\n")

        error_msg = f"\nSQL comparison failed for {test_name}:\n"
        error_msg += f"Expected ({len(expected_lines)} lines):\n{expected_normalized}\n\n"
        error_msg += f"Actual ({len(actual_lines)} lines):\n{actual_normalized}\n\n"

        # Find first difference
        for i, (actual_line, expected_line) in enumerate(zip(actual_lines, expected_lines)):
            if actual_line != expected_line:
                error_msg += f"First difference at line {i + 1}:\n"
                error_msg += f"  Expected: {expected_line}\n"
                error_msg += f"  Actual:   {actual_line}\n"
                break

        if len(actual_lines) != len(expected_lines):
            error_msg += (
                f"Line count mismatch: expected {len(expected_lines)}, got {len(actual_lines)}\n"
            )

        self.fail(error_msg)

    def run_sql_generation_test(self, config_name, from_examples=False):
        """Helper method to run SQL generation test with generate/compare logic.

        Args:
            config_name: Name of the configuration file without extension
            from_examples: If True, load from examples directory instead of test configs
        """
        config = self.load_config(config_name, from_examples)
        actual_sql = generate_sql(config)

        if self.generate_expected:
            if from_examples:
                # Generate expected output in examples directory
                expected_path = self.examples_dir / f"{config_name}.expected.sql"
                with open(expected_path, "w") as f:
                    f.write(actual_sql)
                self.skipTest(
                    f"Generated expected output for example {config_name} in {expected_path}"
                )
            else:
                # Generate expected output in test data directory
                self.save_expected_sql(config_name, actual_sql)
                self.skipTest(f"Generated expected output for {config_name}")
        else:
            # For example files, we compare with expected output in examples directory
            if from_examples:
                expected_path = self.examples_dir / f"{config_name}.expected.sql"
                if not expected_path.exists():
                    # First time running the test, generate the expected output
                    with open(expected_path, "w") as f:
                        f.write(actual_sql)
                    self.skipTest(f"Generated initial expected output for example {config_name}")
                else:
                    # Compare with existing expected output
                    with open(expected_path) as f:
                        expected_sql = f.read().strip()
                    self.compare_sql(actual_sql, expected_sql, f"example_{config_name}")
            else:
                # Normal comparison mode for test configs
                expected_sql = self.load_expected_sql(config_name)
                self.compare_sql(actual_sql, expected_sql, config_name)

    def run_error_test(self, config_name, expected_error_fragment=None):
        """Helper method to run error test with generate/compare logic."""
        config = self.load_config(config_name)

        if self.generate_expected:
            # Generate expected error
            try:
                generate_sql(config)
                # If no error is raised, save empty error file
                self.save_expected_error(config_name, "")
                self.skipTest(f"No error raised for {config_name}, generated empty error file")
            except Exception as e:
                error_message = str(e)
                self.save_expected_error(config_name, error_message)
                self.skipTest(f"Generated expected error for {config_name}: {error_message}")
        else:
            # Normal comparison mode
            expected_error = self.load_expected_error(config_name)

            with self.assertRaises(ValueError) as context:
                generate_sql(config)

            actual_error = str(context.exception)
            if expected_error_fragment:
                self.assertIn(expected_error_fragment, actual_error)
            else:
                self.assertIn(expected_error, actual_error)

    def test_postgres_to_iceberg_basic(self):
        """Test basic PostgreSQL to Iceberg pipeline generation."""
        self.run_sql_generation_test("postgres_to_iceberg_basic")

    def test_postgres_to_iceberg_different_config(self):
        """Test PostgreSQL to Iceberg pipeline with different configuration."""
        self.run_sql_generation_test("postgres_to_iceberg_different_config")

    def test_missing_source_type(self):
        """Test error handling for missing source type."""
        self.run_error_test("missing_source_type")

    def test_missing_sink_type(self):
        """Test error handling for missing sink type."""
        self.run_error_test("missing_sink_type")

    def test_unsupported_source_type(self):
        """Test error handling for unsupported source type."""
        self.run_error_test("unsupported_source_type", "Unsupported connector type: mysql")

    def test_unsupported_sink_type(self):
        """Test error handling for unsupported sink type."""
        self.run_error_test("unsupported_sink_type", "Unsupported connector type: kafka")

    def test_example_job(self):
        """Test the example job.yaml configuration."""
        self.run_sql_generation_test("job", from_examples=True)


class TestConfigurationDiscovery(unittest.TestCase):
    """Test that all configuration files have corresponding expected outputs."""

    def setUp(self):
        """Set up test paths."""
        self.test_dir = Path(__file__).parent
        self.config_dir = self.test_dir / "data" / "configs"
        self.expected_dir = self.test_dir / "data" / "expected"

        # Skip these tests when generating expected outputs
        self.generate_expected = os.environ.get("GENERATE_EXPECTED", "0").lower() in (
            "1",
            "true",
            "yes",
        )

    def test_all_configs_have_expected_outputs(self):
        """Verify that all configuration files have corresponding expected outputs."""
        if self.generate_expected:
            self.skipTest("Skipping validation when generating expected outputs")

        config_files = list(self.config_dir.glob("*.yaml"))

        for config_file in config_files:
            config_name = config_file.stem

            # Check if there's a corresponding .sql or .error file
            sql_file = self.expected_dir / f"{config_name}.sql"
            error_file = self.expected_dir / f"{config_name}.error"

            has_expected_output = sql_file.exists() or error_file.exists()

            self.assertTrue(
                has_expected_output,
                f"Configuration file '{config_file.name}' has no corresponding "
                f"expected output file. Expected either '{sql_file.name}' or "
                f"'{error_file.name}' in {self.expected_dir}",
            )

    def test_all_expected_outputs_have_configs(self):
        """Verify that all expected output files have corresponding configurations."""
        if self.generate_expected:
            self.skipTest("Skipping validation when generating expected outputs")

        sql_files = list(self.expected_dir.glob("*.sql"))
        error_files = list(self.expected_dir.glob("*.error"))

        for output_file in sql_files + error_files:
            config_name = output_file.stem
            config_file = self.config_dir / f"{config_name}.yaml"

            self.assertTrue(
                config_file.exists(),
                f"Expected output file '{output_file.name}' has no corresponding "
                f"configuration file. Expected '{config_file.name}' in {self.config_dir}",
            )

    def test_config_files_are_valid_yaml(self):
        """Test that all configuration files are valid YAML."""
        config_files = list(self.config_dir.glob("*.yaml"))

        for config_file in config_files:
            with self.subTest(config_file=config_file.name):
                try:
                    with open(config_file) as f:
                        yaml.safe_load(f)
                except yaml.YAMLError as e:
                    self.fail(f"Configuration file '{config_file.name}' contains invalid YAML: {e}")


if __name__ == "__main__":
    # Print usage information
    if os.environ.get("GENERATE_EXPECTED", "0").lower() in ("1", "true", "yes"):
        print("=== GENERATE MODE ===")
        print("Generating expected output files instead of comparing.")
        print("Expected files will be written to tests/data/expected/")
        print()
    else:
        print("=== COMPARE MODE ===")
        print("Comparing generated output against expected files.")
        print("Set GENERATE_EXPECTED=1 to generate/update expected output files.")
        print()

    # Run all tests
    unittest.main(verbosity=2)
