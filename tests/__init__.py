"""
Test package bootstrap (backup path).

Every test module also runs an inline bootstrap at the top of the file
(adding the repo root and src/ to sys.path and setting
NLTK_DISABLE_IMPORT_SECURITY=1), because `unittest discover` may import test
modules as top-level names without executing this package __init__. This file
keeps the same setup for package-style invocation
(`python -m unittest tests.test_xxx`) and documents why it exists.

NLTK 3.10+ ships a CWD-import security hook that mis-fires when the virtualenv
lives inside the repository root (every venv module resolves under the current
working directory). The documented escape hatch is NLTK_DISABLE_IMPORT_SECURITY=1.
"""

import os
import sys

os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
