# Databricks notebook source
# MAGIC %sh 
# MAGIC make -f Makefile clean

# COMMAND ----------

# MAGIC %pip install -r requirements-dev.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
from contextlib import redirect_stdout
from io import StringIO

import pytest

# Run all tests in the connected directory in the remote Databricks workspace.
# By default, pytest searches through all files with filenames ending with
# "_test.py" for tests. Within each of these files, pytest runs each function
# with a function name beginning with "test_".

# Get the path to the directory for this file in the workspace.
dir_root = os.path.abspath(".")
print(dir_root)
# Switch to the root directory.
os.chdir(dir_root)

# Skip writing .pyc files to the bytecode cache on the cluster.
sys.dont_write_bytecode = True

# Now run pytest from the root directory, using the

#
temp_stdout = StringIO()
temp_stderr = StringIO()

with redirect_stdout(temp_stdout):
    with redirect_stdout(temp_stderr):  # Redirect stderr as well if needed
        # Call pytest.main() with any desired arguments
        # For example, to run tests in the current directory:
        sys.dont_write_bytecode = True
        result = pytest.main(["-v", "-p", "no:cacheprovider", "test"])

# Retrieve the captured output
stdout_str = temp_stdout.getvalue()
stderr_str = temp_stderr.getvalue()

print("\n--- Captured stdout ---")
print(stdout_str)

print("\n--- Captured stderr ---")
print(stderr_str)

print(f"\nPytest exit code: {result}")
