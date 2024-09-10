# Constant values that are reused across scripts
import argparse
import typing


# Define type for kwargs to argparse's add_argument method, since otherwise mypy
# will be confused by the dict types when we unpack them. See here for details:
# https://stackoverflow.com/a/74316829
class AddArgumentKwargs(typing.TypedDict, total=False):
    action: str | type[argparse.Action]
    default: typing.Any
    help: str


# Definitions for common argparse arguments
TARGET_ARGUMENT_ARGS = ["--target"]
TARGET_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "action": "store",
    "default": "dev",
    "help": "dbt target to use for running commands, defaults to 'dev'",
}
REBUILD_ARGUMENT_ARGS = ["--rebuild"]
REBUILD_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "action": argparse.BooleanOptionalAction,
    "default": False,
    "help": "Rebuild models prior to export",
}
