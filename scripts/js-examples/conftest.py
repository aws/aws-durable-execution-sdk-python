"""Make run.py importable from test_run.py.

The repository's pytest configuration uses --import-mode=importlib, which
does not add a test file's directory to sys.path. So this adds it.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
