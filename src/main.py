"""UNCTAD connector - dynamically discovers and runs all nodes."""
import sys
from pathlib import Path

from subsets_utils import load_nodes, validate_environment

def main():
    validate_environment()
    workflow = load_nodes()
    workflow.run()

if __name__ == "__main__":
    main()
