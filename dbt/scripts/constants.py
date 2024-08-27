import argparse

REFRESH_TABLES_DESCRIPTION = """
The queries that generate these reports run against our data warehouse, which ingests data from iasWorld overnight once daily. Sometimes a
staff member will request a report during the middle of the workday, and they will need the most recent data, which will not exist in
our warehouse yet. In these cases, you can use the --refresh-tables flag to output a command that you can run on the server to refresh
any iasWorld tables that these reports use.
"""  # noqa: E501

REBUILD_CLI_ARGS = ["--rebuild"]
REBUILD_CLI_KWARGS = {
    "action": argparse.BooleanOptionalAction,
    "default": False,
    "dest": "rebuild",
    "help": "Rebuild models before exporting",
}

REFRESH_TABLES_CLI_ARGS = ["--refresh-tables"]
REFRESH_TABLES_CLI_KWARGS = {
    "action": argparse.BooleanOptionalAction,
    "default": False,
    "dest": "refresh_tables",
    "help": (
        "Print a command that can be run on the server to refresh "
        "underlying iasWorld tables and exit. Useful if you want to "
        "refresh table data before running exports"
    ),
}

TARGET_CLI_ARGS = ["--target"]
TARGET_CLI_KWARGS = {
    "required": False,
    "default": "dev",
    "dest": "target",
    "help": "dbt target to use for querying model data, defaults to 'dev'",
}
