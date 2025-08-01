
import pytest
import sys
import os

sys.dont_write_bytecode = True

result = pytest.main(['-vvv', '--import-mode=importlib', '.'])
if result != 0:
    raise Exception(f"Tests failed with exit code: {result}")
