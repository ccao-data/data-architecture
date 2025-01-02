import contextlib
import io
import json
import os
import time

import pandas as pd
import requests
from dbt.cli.main import dbtRunner
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

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


def get_asset_info(socrata_asset):
    """
    Simple helper function to retrieve asset-specific information from dbt.
    """

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
    athena_asset = model.iloc[0]["depends_on.nodes"][0].split(".")[-1]
    asset_id = model.iloc[0]["meta.asset_id"]
    row_identifier = model.iloc[0]["meta.primary_key"]

    return athena_asset, asset_id, row_identifier


def build_query(
    athena_asset, asset_id, row_identifier, years=None, township=None
):
    """
    Build an Athena compatible SQL query. Function will append a year
    conditional if `years` is non-empty. Many of the CCAO's open data assets are
    too large to pass to Socrata without chunking. A `row_id` column is
    constructed in order to use Socrata's `upsert` functionalities (updating
    rather than overwriting data that already exists) based on the column names
    passed to `row_identifiers`.
    """

    row_identifier_sql_parts = [
        f"CAST({col} AS varchar)" for col in row_identifier
    ]
    row_identifier_sql_joined = (
        row_identifier_sql_parts[0]
        if len(row_identifier_sql_parts) == 1
        else f"CONCAT({', '.join(row_identifier_sql_parts)})"
    )
    row_identifier = f"{row_identifier_sql_joined} AS row_id"

    # Retrieve column names and types from Athena
    columns = cursor.execute("show columns from " + athena_asset).as_pandas()

    # Limit pull to columns present in open data asset - shouldn't change anything, but prevents failure if columns have become misaligned.
    asset_columns = (
        requests.get(
            f"https://datacatalog.cookcountyil.gov/resource/{asset_id}"
        )
        .headers["X-SODA2-Fields"]
        .replace('"', "")
        .strip("[")
        .strip("]")
        .split(",")
    )
    columns = columns[columns["column"].isin(asset_columns)]

    # Array type columns are not compatible with the json format needed for
    # Socrata uploads. Automatically convert any array type columns to string.
    columns.loc[columns["type"] == "array(varchar)", "column"] = (
        "ARRAY_JOIN("
        + columns[columns["type"] == "array(varchar)"]["column"]
        + ", ', ') AS "
        + columns[columns["type"] == "array(varchar)"]["column"]
    )

    query = f"SELECT {row_identifier}, {', '.join(columns['column'])} FROM {athena_asset}"

    if not years:
        query = query

    elif years is not None and not township:
        query += " WHERE year = %(year)s"

    elif years is not None and township is not None:
        query += " WHERE year = %(year)s" + " AND township_code = %(township)s"

    return query


def upload(method, asset_id, sql_query, overwrite, year=None, township=None):
    """
    Function to perform the upload to Socrata. `puts` or `posts` depending on
    user's choice to overwrite existing data.
    """

    # Load environmental variables
    app_token = os.getenv("SOCRATA_APP_TOKEN")

    url = "https://datacatalog.cookcountyil.gov/resource/" + asset_id + ".json"

    print_message = "Overwriting" if overwrite else "Updating"

    if not year:
        query_conditionals = {}
        print_message = print_message + " all years for asset " + asset_id
    if year is not None and not township:
        query_conditionals = {"year": year}
        print_message = (
            print_message + " year: " + year + " for asset " + asset_id
        )
    if year is not None and township is not None:
        query_conditionals = {"year": year, "township": township}
        print_message = (
            print_message
            + " township: "
            + township
            + ", year: "
            + year
            + " for asset "
            + asset_id
        )

    # We grab the data before uploading it so we can make sure timestamps are
    # properly formatted
    input_data = cursor.execute(sql_query, query_conditionals).as_pandas()
    date_columns = input_data.select_dtypes(include="datetime").columns
    input_data[date_columns] = input_data[date_columns].map(
        lambda x: x.strftime("%Y-%m-%dT%X")
    )

    # Raise URL status if it's bad
    session.get(
        (
            "https://datacatalog.cookcountyil.gov/resource/"
            + asset_id
            + ".json?$limit=1"
        ),
        headers={"X-App-Token": app_token},
    ).raise_for_status()

    session.get(url=url, headers={"X-App-Token": app_token}).raise_for_status()

    if input_data.shape[0] > 10000:
        for i in range(0, input_data.shape[0], 10000):
            print(print_message)
            print(f"Rows {i + 1}-{i + 10000}")
            response = getattr(session, method)(
                url=url,
                data=input_data.iloc[i : i + 10000].to_json(orient="records"),
                headers={"X-App-Token": app_token},
            )

            print(response.content)

    else:
        print(print_message)
        response = getattr(session, method)(
            url=url,
            data=input_data.to_json(orient="records"),
            headers={"X-App-Token": app_token},
        )

        print(response.content)


def generate_groups(athena_asset, years=None, by_township=False):
    """
    Helper function to determine what groups need to be iterated over for
    upload.
    """

    if not years and by_township:
        raise ValueError("Cannot set 'by_township' when 'years' is None")

    if years == "all":
        years = (
            cursor.execute(
                "SELECT DISTINCT year FROM " + athena_asset + " ORDER BY year"
            )
            .as_pandas()["year"]
            .to_list()
        )

    if by_township:
        township_codes = (
            cursor.execute(
                "SELECT DISTINCT township_code FROM spatial.township"
            )
            .as_pandas()["township_code"]
            .to_list()
        )

        groups = []
        for i in range(len(years)):
            for j in range(len(township_codes)):
                groups.append((years[i], township_codes[j]))

        flag = "both"

    else:
        if not years:
            groups = None
            flag = None
        else:
            groups = years
            flag = "years"

    return flag, groups


def socrata_upload(
    socrata_asset, overwrite=False, years=None, by_township=False
):
    """
    Wrapper function for building SQL query, retrieving data from Athena, and
    uploading it to Socrata. Allows users to specify target Athena and Socrata
    assets, define columns to construct a `row_id`, whether the data on Socrata
    should be overwritten or updated, and whether or not to chunk the upload by
    year. By default the function will query a given Athena asset by year for
    all years and upload via `post` (update rather than overwrite).
    """

    # Github inputs are passed as strings rather than booleans
    if isinstance(overwrite, str):
        overwrite = overwrite == "true"

    if isinstance(by_township, str):
        by_township = by_township == "true"

    athena_asset, asset_id, row_identifier = get_asset_info(socrata_asset)

    flag, groups = generate_groups(
        years=years, by_township=by_township, athena_asset=athena_asset
    )

    tic = time.perf_counter()
    count = 0

    if not flag:
        sql_query = build_query(
            athena_asset=athena_asset,
            asset_id=asset_id,
            row_identifier=row_identifier,
        )

        upload_args = {
            "asset_id": asset_id,
            "sql_query": sql_query,
            "overwrite": overwrite,
        }

        if overwrite:
            upload("put", **upload_args)
        else:
            upload("post", **upload_args)

    else:
        if flag == "years":
            sql_query = build_query(
                athena_asset=athena_asset,
                asset_id=asset_id,
                row_identifier=row_identifier,
                years=years,
            )

        if flag == "both":
            sql_query = build_query(
                athena_asset=athena_asset,
                asset_id=asset_id,
                row_identifier=row_identifier,
                years=years,
                township=by_township,
            )

        for item in groups:
            if flag == "both":
                upload_args = {
                    "asset_id": asset_id,
                    "sql_query": sql_query,
                    "overwrite": overwrite,
                    "year": item[0],
                    "township": item[1],
                }
            elif flag == "years":
                upload_args = {
                    "asset_id": asset_id,
                    "sql_query": sql_query,
                    "overwrite": overwrite,
                    "year": item,
                }
            if count == 0 and overwrite:
                upload("put", **upload_args)
            else:
                upload("post", **upload_args)
            count = count + 1

    toc = time.perf_counter()
    print(f"Total upload in {toc - tic:0.4f} seconds")


socrata_upload(
    socrata_asset=os.getenv("SOCRATA_ASSET"),
    overwrite=os.getenv("OVERWRITE"),
    years=str(os.getenv("YEARS")).split(","),
    by_township=os.getenv("BY_TOWNSHIP"),
)
