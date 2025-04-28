#!/usr/bin/env python3
# Run tests on our open data assets to confirm that their row counts match
# the row counts of the underlying Athena tables when grouped by year.
#
# Requires one positional argument, the path to a dbt manifest that can be
# used to parse exposure data representing open data assets and their
# dependency graph.
#
# Exposures that should be tested are required to have a `meta.test_row_count`
# attribute set to `true`. Exposures that do not have this attribute set to
# `true` will not be tested.
#
# Example usage:
#
#   cd dbt
#   python3 ../.github/scripts/test_open_data_assets.py target/manifest.json

import contextlib
import datetime
import io
import json
import sys
import typing

import agate
import requests
from dbt.cli.main import dbtRunner

# API URL and params that we can use to grab counts by year for each asset
ASSET_API_URL = "https://datacatalog.cookcountyil.gov/resource/{asset_id}.json"
ASSET_API_QUERY = "SELECT COUNT(*),{year_field} GROUP BY {year_field}"
DEFAULT_YEAR_FIELD = "year"
DBT = dbtRunner()
REQUESTS = requests.Session()

# Data for the most recent two years are permitted to be slightly different,
# according to this buffer value. This is because we expect some level of
# change in the Athena data (which is updated daily) compared to the Open Data
# portal (which is updated bi-weekly or monthly).
BUFFER = 0.02


def main() -> None:
    """Entrypoint for the script."""
    dbt_manifest_path = sys.argv[1]
    with open(dbt_manifest_path) as dbt_manifest_file:
        dbt_manifest_json = json.loads(dbt_manifest_file.read())

    diffs: typing.List[typing.List[typing.Dict]] = []

    for exposure in dbt_manifest_json["exposures"].values():
        exposure_meta = exposure.get("meta", {})
        if not exposure_meta.get("test_row_count", False):
            print(
                f"Skipping row count test for exposure `{exposure['name']}` "
                "because it does not have an enabled `meta.test_row_count` "
                "attribute"
            )
            continue

        asset_name = exposure["label"]
        asset_url = exposure["url"]
        asset_id = asset_url.split("/")[-1]
        asset_year_field = exposure_meta.get("year_field", DEFAULT_YEAR_FIELD)

        # For now, assume just one dependency table, since all of our open
        # data assets currently have a 1:1 relationship with a view.
        fq_source_model_name = exposure["depends_on"]["nodes"][0]

        # Dependency tables in the DAG are fully qualified, so we have to
        # strip the prefix to get the schema and tablename.
        source_model_name = ".".join(fq_source_model_name.split(".")[2:])

        print(f"Comparing asset '{asset_name}' to model '{source_model_name}'")

        asset_api_url = ASSET_API_URL.format(asset_id=asset_id)
        asset_api_query = ASSET_API_QUERY.format(year_field=asset_year_field)
        asset_api_params = {"$query": asset_api_query}
        response = REQUESTS.get(asset_api_url, params=asset_api_params)
        if not response.ok:
            print("Encountered error in request to asset endpoint")
            response.raise_for_status()

        asset_row_counts_by_year = response.json()

        # Socrata won't return a year column for rows with no year, so we need
        # to add None as the year value to any rows with no year. Also,
        # Socrata returns year columns as strings even though they're typed as
        # numbers (it's not clear why the API behaves this way), so we need to
        # convert them to int in order to compare them to Athena data.
        asset_row_counts_by_year = [
            {
                **year_count,
                asset_year_field: int(year_count[asset_year_field])
                if year_count.get(asset_year_field) is not None
                else None
            }
            for year_count in asset_row_counts_by_year
        ]

        dbt_output = io.StringIO()
        with contextlib.redirect_stdout(dbt_output):
            dbt_result = DBT.invoke(
                [
                    "--quiet",
                    "run-operation",
                    "row_count_by_group",
                    "--args",
                    (
                        "{"
                        f"model: '{source_model_name}', "
                        "group_by: 'year', "
                        "print: true"
                        "}"
                    ),
                    "--target",
                    "prod",
                ]
            )

        if not dbt_result.success:
            print("Encountered error in dbt call")
            raise ValueError(dbt_result.exception)

        source_model_row_counts_by_year = json.loads(dbt_output.getvalue())

        if diff := diff_row_counts(
            source_model_name,
            source_model_row_counts_by_year,
            asset_name,
            asset_row_counts_by_year,
            open_data_asset_year_field=asset_year_field,
        ):
            diffs.append(diff)

    if diffs:
        print()
        print(
            "The following view/asset pairs had mismatching row counts "
            "by year:"
        )
        for diff in diffs:
            print()
            diff_table = agate.Table.from_object(diff)
            # Note that agate adds thousands separators to years, even when
            # they're strings (as in our data). This is super annoying but
            # it's still the fastest way we know to print a clean table
            # in Python :\
            diff_table.print_table(
                max_rows=None, max_columns=None, max_column_width=None
            )
        print()
        raise ValueError("Open data asset test failed")

    print("All open data assets have the expected row counts by year!")


def diff_row_counts(
    athena_model_name: str,
    athena_model_row_counts: typing.List[typing.Dict],
    open_data_asset_name: str,
    open_data_asset_row_counts: typing.List[typing.Dict],
    athena_model_year_field: str = DEFAULT_YEAR_FIELD,
    open_data_asset_year_field: str = DEFAULT_YEAR_FIELD,
    current_year_buffer: float = BUFFER,
) -> typing.List[typing.Dict]:
    """Check whether two lists of row count dicts are the same. Applies a
    buffer to the current year of data to account for the fact that more
    recent years may have incomplete data.

    Returns a list of dicts representing the rows that differ in each
    dataset. If no rows differ, the return value will be an empty list.

    Row count dicts are expected to have a "COUNT" key and a `year_field` key.
    The "COUNT" key should represent an integer, but it can be a string
    representation of an int in the case of open_data_asset_row_counts,
    since that is the format returned by the Socrata API.
    """
    # We expect these lists to already be sorted, but double-check anyway
    athena_model_row_counts = sorted(
        athena_model_row_counts,
        key=lambda x: (
            x[athena_model_year_field]
            if x[athena_model_year_field] is not None
            else 0
        ),
    )
    open_data_asset_row_counts = sorted(
        open_data_asset_row_counts,
        key=lambda x: (
            x[open_data_asset_year_field]
            if x[open_data_asset_year_field] is not None
            else 0
        ),
    )

    # Extract the current and last year since we want to apply different
    # matching rules to them
    current_year_int = datetime.datetime.now().year
    last_year_int = current_year_int - 1
    current_year, last_year = str(current_year_int), str(last_year_int)

    formatted_rows: typing.List[typing.Dict] = []

    for model_row_count_dict in athena_model_row_counts:
        model_year = model_row_count_dict[athena_model_year_field]
        model_count = model_row_count_dict["COUNT"]
        base_formatted_row = {
            athena_model_year_field: model_year,
            athena_model_name: model_count,
        }

        asset_count: int = 0
        for asset_row_count_dict in open_data_asset_row_counts:
            if asset_row_count_dict[open_data_asset_year_field] == model_year:
                asset_count = int(asset_row_count_dict["COUNT"])
                break
        else:
            # No matching year found, so these two datasets must be different
            formatted_rows.append(
                {**base_formatted_row, **{open_data_asset_name: None}}
            )
            continue

        counts_match = (
            (
                model_count * (1 - current_year_buffer)
                < asset_count
                < model_count * (1 + current_year_buffer)
            )
            if model_year in [current_year, last_year]
            else asset_count == model_count
        )

        if not counts_match:
            formatted_rows.append(
                {**base_formatted_row, **{open_data_asset_name: asset_count}}
            )

    # Reverse the comparison to check for years that are in open data, but not
    # in the model. Note that we don't have to check for equality here, because
    # anything that was missed in the previous iteration will be null on the
    # model side, hence a mismatch by definition
    for asset_row_count_dict in open_data_asset_row_counts:
        asset_year = asset_row_count_dict[open_data_asset_year_field]
        asset_count = asset_row_count_dict["COUNT"]
        base_formatted_row = {}

        for model_row_count_dict in athena_model_row_counts:
            if model_row_count_dict[athena_model_year_field] == asset_year:
                break
        else:
            # No matching year found, so these two datasets must be different
            formatted_rows.append(
                {
                    athena_model_year_field: asset_year,
                    athena_model_name: None,
                    open_data_asset_name: asset_count,
                }
            )

    return formatted_rows


if __name__ == "__main__":
    main()
