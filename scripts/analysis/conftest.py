import os
import sys

# Make scripts/algorithms.py and the analysis modules importable in tests.
HERE = os.path.dirname(os.path.abspath(__file__))
for p in (HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)
