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
Cross-platform test runner for RisingWave CDC project.

This script handles virtual environment activation and runs all tests
with proper error handling and output formatting.
"""

import platform
import subprocess
import sys
from pathlib import Path


def find_venv_directory():
    """Find the virtual environment directory."""
    possible_venvs = [".venv", "venv"]
    for venv_name in possible_venvs:
        if Path(venv_name).is_dir():
            return Path(venv_name)
    return None


def get_venv_python(venv_dir):
    """Get the path to the Python executable in the virtual environment."""
    if platform.system() == "Windows":
        return venv_dir / "Scripts" / "python.exe"
    else:
        return venv_dir / "bin" / "python"


def get_venv_pip(venv_dir):
    """Get the path to the pip executable in the virtual environment."""
    if platform.system() == "Windows":
        return venv_dir / "Scripts" / "pip.exe"
    else:
        return venv_dir / "bin" / "pip"


def run_command(cmd, description, capture_output=False):
    """Run a command with error handling."""
    try:
        if capture_output:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout
        else:
            subprocess.run(cmd, check=True)
            return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to {description}")
        if capture_output and e.stderr:
            print(f"Error: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"❌ Command not found while trying to {description}")
        return False


def main():
    """Main test runner function."""
    print("🚀 RisingWave CDC Test Runner (Python)")
    print("=====================================")

    # Check if virtual environment exists
    venv_dir = find_venv_directory()
    if not venv_dir:
        print("❌ No virtual environment found!")
        print("Please create a virtual environment first:")
        print("  python3 -m venv .venv")
        if platform.system() == "Windows":
            print("  .venv\\Scripts\\activate")
        else:
            print("  source .venv/bin/activate")
        print("  pip install -r requirements.txt")
        return 1

    print(f"📦 Found virtual environment: {venv_dir}")

    # Get paths to venv executables
    venv_python = get_venv_python(venv_dir)
    venv_pip = get_venv_pip(venv_dir)

    if not venv_python.exists():
        print(f"❌ Python executable not found in virtual environment: {venv_python}")
        return 1

    print(f"✅ Using Python: {venv_python}")

    # Install/update dependencies
    print("📋 Installing/updating dependencies...")
    if not run_command(
        [str(venv_pip), "install", "-r", "requirements.txt"],
        "install dependencies",
        capture_output=True,
    ):
        print("⚠️  Warning: Failed to install dependencies, continuing anyway...")

    # Optionally install pytest for better output
    run_command([str(venv_pip), "install", "pytest"], "install pytest", capture_output=True)

    print("🧪 Running tests...")
    print("=================")

    # Track test results
    all_tests_passed = True

    # Run data-driven tests
    print("📊 Running data-driven tests...")
    if not run_command([str(venv_python), "tests/test_data_driven.py"], "run data-driven tests"):
        all_tests_passed = False

    print()

    # Summary
    if all_tests_passed:
        print("✅ All tests completed successfully!")
        print("=" * 40)
        print("🏁 Test run finished.")
        return 0
    else:
        print("❌ Some tests failed!")
        print("=" * 40)
        print("🏁 Test run finished with errors.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
