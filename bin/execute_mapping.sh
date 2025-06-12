#!/bin/bash

VERSION="0.1.0"
DIT_HOME="<DIT_HOME>"
DIT_TOKEN="<DIT_TOKEN>"

export DIT_HOME
export DIT_TOKEN

print_help() {
cat <<EOF
D.I.T - Data Integrator Tool

Usage:
  execute_mapping.sh <command> [options]

Commands:
  execute        Execute a mapping
  describe       Describe a mapping

Options:
  -h, --help     Show this help message
  -v, --version  Show version info
EOF
}

print_version() {
    echo "D.I.T version $VERSION"
}

# Parse top-level options
case "$1" in
    -h|--help)
        print_help
        exit 0
        ;;
    -v|--version)
        print_version
        exit 0
        ;;
esac

# Subcommand dispatcher
COMMAND=$1
shift  # shift to access arguments to subcommand

case "$COMMAND" in
    execute)
        if [ -z "$1" ]; then
          echo "Missing mapping directory"
          echo "Usage: describe <mapping_file_directory>"
          exit 1
        fi
        python3 $DIT_HOME/python/execute_mapping.py "$1" ${@:2}
        ;;
    describe)
        if [ -z "$1" ]; then
          echo "Missing mapping directory"
          echo "Usage: describe <mapping_file_directory>"
          exit 1
        fi
        python3 $DIT_HOME/python/describe_mapping.py "$1" ${@:2}
        ;;
    *)
        echo "Unknown command: $COMMAND"
        print_help
        exit 1
        ;;
esac