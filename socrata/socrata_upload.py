import contextlib
import io
import json
import logging
import os
import re
import time
from datetime import datetime
from urllib.parse import quote

import pandas as pd
import requests
from dbt.cli.main import dbtRunner
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

logger = logging.getLogger(__name__)

# Allow python to print full length dataframes for logging
pd.set_option("display.max_rows", None)

# Create a session object so HTTP requests can be pooled
session = requests.Session()
session.auth = (
    str(os.getenv("SOCRATA_USERNAME")),
    str(os.getenv("SOCRATA_PASSWORD")),
)

# Connect to Athena
cursor = connect(
    s3_staging_dir="s3://ccao-athena-results-us-east-1/",
    region_name="us-east-1",
    cursor_class=PandasCursor,
).cursor(unload=True)


def parse_assets(assets=None):
    """
    Retrieve metadata for all socrata assets and filter that data based
    on parsed input assets names.
    """

    if assets == "":
        assets = None

    # When running locally, we will probably be inside the socrata/ dir, so
    # switch back out to find the dbt/ dir
    if not os.path.isdir("./dbt"):
        os.chdir("..")

    os.chdir("./dbt")

    DBT = dbtRunner()
    dbt_list_args = [
        "--quiet",
        "list",
        "--select",
        "open_data.*",
        "--resource-types",
        "exposure",
        "--exclude",
        "tag:inactive",
        "--output",
        "json",
        "--output-keys",
        "label",
        "meta",
        "depends_on",
    ]

    print(f"> dbt {' '.join(dbt_list_args)}")
    dbt_output = io.StringIO()
    with contextlib.redirect_stdout(dbt_output):
        DBT.invoke(dbt_list_args)

    model = [
        json.loads(model_dict_str)
        for model_dict_str in dbt_output.getvalue().split("\n")
        # Filter out empty strings caused by trailing newlines
        if model_dict_str
    ]

    os.chdir("..")

    all_assets = pd.json_normalize(model)

    # Split names of views in athena to remove extaneous db prefixes
    all_assets["athena_asset"] = (
        all_assets["depends_on.nodes"].str[0].str.split(pat=".", n=2).str[-1]
    )

    all_assets = all_assets[["label", "meta.asset_id", "athena_asset"]].rename(
        columns={"meta.asset_id": "asset_id"}
    )

    # If no assets are entered return metadata for all assets otherwise filter
    # by provided assets
    if assets is not None:
        assets = [asset.strip() for asset in str(assets).split(",")]

        all_assets = all_assets[all_assets["label"].isin(assets)]

    print("Assets that will be updated:")
    print(all_assets["label"])

    # Return a dict with labels as keys
    all_assets = all_assets.set_index("label").to_dict("index")

    return all_assets


def parse_years(years=None):
    """
    Make sure the years environmental variable is formatted correctly.
    """

    if years == "":
        years = None
    if years is not None:
        if years == "all":
            years = ["all"]
        else:
            # Only allow anticipated values
            years = [
                re.sub("[^0-9]", "", year)
                for year in str(years).split(",")
                if re.sub("[^0-9]", "", year)
            ]

    return years


def parse_years_list(athena_asset, years=None):
    """
    Helper function to determine what years need to be iterated over for
    upload.
    """

    if years is not None:
        if years == ["all"]:
            years_list = (
                cursor.execute(
                    "SELECT DISTINCT year FROM "
                    + athena_asset
                    + " ORDER BY year"
                )
                .as_pandas()["year"]
                .to_list()
            )
        else:
            years_list = years

    elif not years and os.getenv("WORKFLOW_EVENT_NAME") == "schedule":
        # Update most recent year only on scheduled workflow. In some
        # cases the max year is incorrectly in the future, so we
        # append the current year to the list and take the minimum.
        years_list = (
            cursor.execute("SELECT MAX(year) AS year FROM " + athena_asset)
            .as_pandas()["year"]
            .to_list()
        )
        years_list.append(datetime.now().year)
        years_list = [min(years_list)]

    else:
        years_list = None

    return years_list


def check_overwrite(overwrite=None):
    """
    Make sure overwrite environmental variable is typed correctly.
    """

    if not overwrite or overwrite == "":
        overwrite = False

    # Github inputs are passed as strings rather than booleans
    if isinstance(overwrite, str):
        overwrite = overwrite == "true"

    return overwrite


def build_query_dict(athena_asset, asset_id, years=None):
    """
    Build a dictionary of Athena compatible SQL queries and their associated
    years. Many of the CCAO's open data assets are too large to pass to Socrata
    without chunking.
    """

    # Retrieve column names and types from Athena
    columns = cursor.execute("show columns from " + athena_asset).as_pandas()
    athena_columns = columns["column"].tolist()
    athena_columns.sort()

    # Retrieve column names from Socrata
    asset_columns = (
        session.get(
            f"https://datacatalog.cookcountyil.gov/resource/{asset_id}"
        )
        .headers["X-SODA2-Fields"]
        .replace('"', "")
        .strip("[")
        .strip("]")
        .split(",")
    )
    # row id won't show up here since it's hidden on the open data portal assets
    asset_columns += ["row_id"]
    asset_columns.sort()

    # If there are columns on Socrata that are not in Athena, abort upload and
    # inform user of discrepancies. The script should not run at all in this
    # circumstance since it will update some but not all columns in the open
    # data asset.
    # If there are columns in Athena but not on Socrata, it may be the case that
    # they should be added, but there are also cases when not all columns for an
    # Athena view that feeds an open data asset need to be part of that asset.
    if athena_columns != asset_columns:
        columns_not_on_socrata = set(athena_columns) - set(asset_columns)
        columns_not_in_athena = set(asset_columns) - set(athena_columns)
        exception_message = (
            f"Columns on Socrata and in Athena do not match for {athena_asset}"
        )

        if len(columns_not_on_socrata) > 0:
            exception_message += f"\nColumns in Athena but not on Socrata: {columns_not_on_socrata}"
            logger.warning(exception_message)
        if len(columns_not_in_athena) > 0:
            exception_message += f"\nColumns on Socrata but not in Athena: {columns_not_in_athena}"
            raise Exception(exception_message)

    # Limit pull to columns present in open data asset
    columns = columns[columns["column"].isin(asset_columns)]

    print(f"The following columns will be updated for {athena_asset}:")
    print(columns)

    query = f"""SELECT {", ".join(columns["column"])} FROM {athena_asset}"""

    # Build a dictionary with queries for each year requested, or no years
    if not years:
        query_dict = {None: query}

    else:
        query_dict = {year: f"{query} WHERE year = {year}" for year in years}

    return query_dict


def check_deleted(input_data, asset_id, app_token):
    """
    Download row_ids from Socrata asset to check for rows still present in
    Socrata that have been deleted in Athena. If any are found, they will be
    passed to Socrata with the ":deleted" column set to true.
    """

    # Determine which years are present in the input data. We only want to
    # retrieve row_ids for the corresponding years from Socrata.
    years = [str(year) for year in input_data["year"].unique().tolist()]
    years = ", ".join(years)

    # Construct the API call to retrieve row_ids for the specified asset and years
    url = (
        f"https://datacatalog.cookcountyil.gov/resource/{asset_id}.json?$query="
        + quote(f"SELECT row_id WHERE year IN ({years}) LIMIT 20000000")
    )

    # Retrieve row_ids from Socrata for the specified asset and years
    socrata_rows = pd.DataFrame(
        session.get(url=url, headers={"X-App-Token": app_token}).json()
    )

    # Outer-join the input data with Socrata data to find rows that are
    # present in Socrata but not in the Athena input data. For those rows set
    # the ":deleted" column to True.
    input_data = input_data.merge(
        socrata_rows, on="row_id", how="outer", indicator=True
    )
    input_data[":deleted"] = None
    input_data.loc[input_data["_merge"] == "right_only", ":deleted"] = True
    input_data = input_data.drop(columns=["_merge"])

    return input_data


def upload(asset_id, sql_query, overwrite):
    """
    Function to perform the upload to Socrata. `puts` or `posts` depending on
    user's choice to overwrite existing data.
    """

    # Load environmental variables
    app_token = os.getenv("SOCRATA_APP_TOKEN")

    url = "https://datacatalog.cookcountyil.gov/resource/" + asset_id + ".json"

    # Raise URL status if it's bad
    session.get(url=url, headers={"X-App-Token": app_token}).raise_for_status()

    # We grab the data before uploading it so we can make sure timestamps are
    # properly formatted
    for year, query in sql_query.items():
        print_message = "Overwriting" if overwrite else "Updating"

        if not year:
            print_message = print_message + " all years for asset " + asset_id
        else:
            print_message = (
                f"{print_message} year: {year} for asset {asset_id}"
            )

        input_data = cursor.execute(query).as_pandas()

        # Ensure rows that need to be deleted from Socrata are marked as such
        input_data = check_deleted(input_data, asset_id, app_token)

        date_columns = input_data.select_dtypes(include="datetime").columns
        for i in date_columns:
            input_data[i] = input_data[i].fillna("").dt.strftime("%Y-%m-%dT%X")

        for i in range(0, input_data.shape[0], 10000):
            print(print_message)
            print(f"Rows {i + 1}-{i + 10000}")
            method = "post" if not overwrite else "put"
            response = getattr(session, method)(
                url=url,
                data=input_data.iloc[i : i + 10000].to_json(orient="records"),
                headers={"X-App-Token": app_token},
            )
            overwrite = False
            print(response.content)

        overwrite = False


def socrata_upload(asset_info, overwrite=False, years=None):
    """
    Wrapper function for building SQL query, retrieving data from Athena, and
    uploading it to Socrata. Allows users to specify target Athena and Socrata
    assets, whether the data on Socrata should be overwritten or updated, and
    whether or not to chunk the upload by year. By default the function will
    query a given Athena asset by year for all years and upload via `post`
    (update rather than overwrite).
    """

    athena_asset, asset_id = asset_info["athena_asset"], asset_info["asset_id"]

    years_list = parse_years_list(years=years, athena_asset=athena_asset)

    sql_query = build_query_dict(
        athena_asset=athena_asset,
        asset_id=asset_id,
        years=years_list,
    )

    tic = time.perf_counter()
    upload(asset_id, sql_query, overwrite)
    toc = time.perf_counter()

    print(f"Total upload in {toc - tic:0.4f} seconds")


if __name__ == "__main__":
    print(f"Running upload for event type: {os.getenv('WORKFLOW_EVENT_NAME')}")

    # Retrieve asset(s)
    all_assets = parse_assets(os.getenv("SOCRATA_ASSET"))

    for asset_info in all_assets.values():
        socrata_upload(
            asset_info=asset_info,
            overwrite=check_overwrite(os.getenv("OVERWRITE")),
            years=parse_years(os.getenv("YEARS")),
        )
