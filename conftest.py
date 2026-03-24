"""
conftest.py
Pytest configuration — ensures the project root is on sys.path
so all modules resolve correctly during testing.
"""

import sys
from pathlib import Path

# Add project root to path so `from pipeline.x import y` works in tests
sys.path.insert(0, str(Path(__file__).resolve().parent))
