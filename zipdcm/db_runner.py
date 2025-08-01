import pytest
import os
import sys

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
retcode = pytest.main(["-v", "."])
dbutils.notebook.exit(retcode)