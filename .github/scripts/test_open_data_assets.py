#!/usr/bin/env python3
# Run tests on our open data assets
import contextlib
import datetime
import io
import json
import pprint
import sys
import typing

import requests
from dbt.cli.main import dbtRunner

# API URL and params that we can use to grab counts by year for each asset
ASSET_API_URL = "https://datacatalog.cookcountyil.gov/resource/{asset_id}.json"
ASSET_API_QUERY_PARAMS = {"$query": "SELECT COUNT(*),year GROUP BY year"}
DBT = dbtRunner()

# Data for the most recent two years are permitted to be slightly different,
# according to this buffer value
BUFFER = 0.2


def main() -> None:
    """Entrypoint for the script."""
    dbt_manifest_path = sys.argv[1]
    with open(dbt_manifest_path) as dbt_manifest_file:
        dbt_manifest_json = json.loads(dbt_manifest_file.read())

    errors: typing.List[typing.Dict] = []

    for exposure in dbt_manifest_json["exposures"].values():
        asset_name = exposure["label"]
        asset_url = exposure["url"]
        asset_id = asset_url.split("/")[-1]

        # For now, assume just one dependency table, since all of our open
        # data assets currently have a 1:1 relationship with a view.
        fq_source_model_name = exposure["depends_on"]["nodes"][0]

        # Dependency tables in the DAG are fully qualified, so we have to
        # strip the prefix to get the schema and tablename.
        source_model_name = ".".join(fq_source_model_name.split(".")[2:])

        print(f"Comparing asset '{asset_name}' to model '{source_model_name}'")

        asset_api_url = ASSET_API_URL.format(asset_id=asset_id)
        response = requests.get(asset_api_url, params=ASSET_API_QUERY_PARAMS)
        if not response.ok:
            print("Encountered error in request to asset endpoint")
            response.raise_for_status()

        asset_row_counts_by_year = response.json()

        dbt_output = io.StringIO()
        with contextlib.redirect_stdout(dbt_output):
            dbt_result = DBT.invoke(
                [
                    "--quiet",
                    "run-operation",
                    "row_count_by_year",
                    "--args",
                    f"{{model: '{source_model_name}', print: true}}",
                    "--target",
                    "prod"
                ]
            )

        if not dbt_result.success:
            print("Encountered error in dbt call")
            raise ValueError(dbt_result.exception)

        source_model_row_counts_by_year = json.loads(dbt_output.getvalue())

        if row_counts_differ(
            source_model_row_counts_by_year, asset_row_counts_by_year
        ):
            errors.append(
                {
                    source_model_name: source_model_row_counts_by_year,
                    asset_name: asset_row_counts_by_year,
                }
            )

    if errors:
        print()
        print(
            "The following view/asset pairs did not have matching row "
            "counts by year:"
        )
        print()
        for error in errors:
            pprint.pprint(error)
        print()
        raise ValueError("Open data asset test failed")

    print("All open data assets have the expected row counts by year!")


def row_counts_differ(
    athena_model_row_counts: typing.List[typing.Dict],
    open_data_asset_row_counts: typing.List[typing.Dict],
    current_year_buffer: float = BUFFER,
) -> bool:
    """Check whether two lists of row count dicts are the same. Applies a
    buffer to the current year of data to account for the fact that more
    recent years may have incomplete data.

    Row count dicts are expected to have a "COUNT" key and a "year" key.
    The "COUNT" key should represent an integer, but it can be a string
    representation of an int in the case of open_data_asset_row_counts,
    where that is the format returned by the Socrata API.
    """
    # We expect these lists to already be sorted, but double-check anyway
    athena_model_row_counts = sorted(
        athena_model_row_counts, key=lambda x: x["year"]
    )
    open_data_asset_row_counts = sorted(
        open_data_asset_row_counts, key=lambda x: x["year"]
    )

    # Extract the current and last year since we want to apply different
    # matching rules to them
    current_year = datetime.datetime.now().year
    last_year = current_year - 1
    current_year, last_year = str(current_year), str(last_year)

    for model_row_count_dict in athena_model_row_counts:
        model_year = model_row_count_dict["year"]
        model_count = model_row_count_dict["COUNT"]

        asset_count: int = 0
        for asset_row_count_dict in open_data_asset_row_counts:
            if asset_row_count_dict["year"] == model_year:
                asset_count = int(asset_row_count_dict["COUNT"])
                break
        else:
            # No matching year found, so these two datasets must be different
            return True

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
            return True

    return False


if __name__ == "__main__":
    main()
