import contextlib
import io
import json
import logging
import os
import time

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


def parse_years(years):
    """
    Make sure the years environmental variable is formatted correctly.
    """

    if years == "":
        years = None
    if years is not None:
        years = str(years).replace(" ", "").split(",")

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

    else:
        years_list = None

    return years_list


def check_overwrite(overwrite):
    """
    Make sure overwrite environmental variable is typed correctly.
    """

    if not overwrite:
        overwrite = False

    # Github inputs are passed as strings rather than booleans
    if isinstance(overwrite, str):
        overwrite = overwrite == "true"

    return overwrite


def get_asset_info(socrata_asset):
    """
    Simple helper function to retrieve asset-specific information from dbt.
    """

    if not os.path.isdir("./dbt"):
        os.chdir("..")

    os.chdir("./dbt")

    DBT = dbtRunner()
    dbt_list_args = [
        "--quiet",
        "list",
        "--resource-types",
        "exposure",
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

    model = pd.json_normalize(model)
    model = model[model["label"] == socrata_asset]
    athena_asset = model.iloc[0]["depends_on.nodes"][0].split(".", 2)[-1]
    asset_id = model.iloc[0]["meta.asset_id"]

    return athena_asset, asset_id


def build_query(athena_asset, asset_id, years=None):
    """
    Build an Athena compatible SQL query. Function will append a year
    conditional if `years` is non-empty. Many of the CCAO's open data assets are
    too large to pass to Socrata without chunking.
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

    print("The following columns will be updated:")
    print(columns)

    query = f"SELECT {', '.join(columns['column'])} FROM {athena_asset}"

    # Build a dictionary with queries for each year requested, or no years
    if not years:
        query = {None: query}

    else:
        query = [query + " WHERE year = '" + year + "'" for year in years]
        query = dict([(k, v) for k, v in zip(years, query)])

    return query


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
                print_message + " year: " + year + " for asset " + asset_id
            )
        print(year)
        print(query)
        input_data = cursor.execute(query).as_pandas()
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


def socrata_upload(socrata_asset, overwrite=False, years=None):
    """
    Wrapper function for building SQL query, retrieving data from Athena, and
    uploading it to Socrata. Allows users to specify target Athena and Socrata
    assets, whether the data on Socrata should be overwritten or updated, and
    whether or not to chunk the upload by year. By default the function will
    query a given Athena asset by year for all years and upload via `post`
    (update rather than overwrite).
    """

    athena_asset, asset_id = get_asset_info(socrata_asset)

    years_list = parse_years_list(years=years, athena_asset=athena_asset)

    sql_query = build_query(
        athena_asset=athena_asset,
        asset_id=asset_id,
        years=years_list,
    )

    tic = time.perf_counter()
    upload(asset_id, sql_query, overwrite)
    toc = time.perf_counter()

    print(f"Total upload in {toc - tic:0.4f} seconds")


socrata_upload(
    socrata_asset=os.getenv("SOCRATA_ASSET"),
    overwrite=check_overwrite(os.getenv("OVERWRITE")),
    years=parse_years(os.getenv("YEARS")),
)
