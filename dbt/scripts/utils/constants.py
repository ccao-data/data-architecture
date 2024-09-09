# Constant values that are reused across scripts
import argparse

# Definitions for common argparse arguments
TARGET_ARGUMENT_ARGS = ["--target"]
TARGET_ARGUMENT_KWARGS = {
    "required": False,
    "default": "dev",
    "help": "dbt target to use for running commands, defaults to 'dev'",
}
REBUILD_ARGUMENT_ARGS = ["--rebuild"]
REBUILD_ARGUMENT_KWARGS = {
    "action": argparse.BooleanOptionalAction,
    "default": False,
    "help": "Rebuild models prior to export",
}
