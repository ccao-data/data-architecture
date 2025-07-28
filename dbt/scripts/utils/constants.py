# Constant values that are reused across scripts
import argparse
import typing


# Define type for kwargs to argparse's add_argument method, since otherwise mypy
# will be confused by the dict types when we unpack them. The `total=False`
# kwarg marks all of these keys as optional. See here for details:
# https://stackoverflow.com/a/74316829
class AddArgumentKwargs(typing.TypedDict, total=False):
    action: str | type[argparse.Action]
    default: typing.Any
    nargs: str | int
    help: str


# Definitions for common argparse arguments
TARGET_ARGUMENT_ARGS = ["--target"]
TARGET_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "default": "dev",
    "help": "dbt target to use for running commands, defaults to 'dev'",
}
REBUILD_ARGUMENT_ARGS = ["--rebuild"]
REBUILD_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "action": argparse.BooleanOptionalAction,
    "default": False,
    "help": "Rebuild models prior to export",
}
OUTPUT_DIR_ARGUMENT_ARGS = ["--output-dir"]
OUTPUT_DIR_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "help": (
        "Unix-formatted path to directory where output files will be stored, "
        "defaults to export/output/"
    ),
}
SELECT_ARGUMENT_ARGS = ["--select"]
SELECT_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "nargs": "*",
    "help": (
        "One or more dbt select statements to use for filtering models. "
        "The --selector arg will take precedence over this one if it's present"
    ),
}
SELECTOR_ARGUMENT_ARGS = ["--selector"]
SELECTOR_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "help": (
        "A selector name to use for filtering models, as defined in "
        "selectors.yml. Takes precedence over --select when both are present"
    )
}
LOG_TO_FILE_ARGUMENT_ARGS = ["--log-to-file"]
LOG_TO_FILE_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "help": (
        "A flag to indicate whether to log to a file. If set, logs will be "
        "written to a user-defined location."
    )
}
LOG_TO_CLOUDWATCH_GROUP_ARGUMENT_ARGS = ["--log-to-cloudwatch-group"]
LOG_TO_CLOUDWATCH_GROUP_ARGUMENT_KWARGS: AddArgumentKwargs = {
    "help": (
        "A flag to indicate whether to log to a CloudWatch group. If set, logs"
        "will be written to a user-defined AWS CloudWatch group."
    )
}
