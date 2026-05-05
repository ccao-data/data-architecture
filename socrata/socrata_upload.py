import contextlib
import io
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
from dbt.cli.main import dbtRunner
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)

# Allow python to print full length dataframes for logging
pd.set_option("display.max_rows", None)

# Create a session object so HTTP requests can be pooled
session = requests.Session()
session.headers.update({"X-App-Token": str(os.getenv("SOCRATA_APP_TOKEN"))})
session.auth = (
    str(os.getenv("SOCRATA_USERNAME")),
    str(os.getenv("SOCRATA_PASSWORD")),
)

# Configure retries for requests to the open data portal API since it has a
# tendency to return 500 errors under load. We need to do this via a custom
# `Retry` instance because `requests` does not retry connection errors or
# requests where data has made it to the server unless you use a custom
# `Retry` instance to override that behavior
retries = Retry(
    total=5,
    backoff_factor=0.1,
    status_forcelist=[500, 502, 503, 504],
    # Set default allowed methods for retries to include POST, since the Socrata API
    # uses idempotent POST requests
    allowed_methods=Retry.DEFAULT_ALLOWED_METHODS | {"POST"},
)

session.mount(
    "https://datacatalog.cookcountyil.gov", HTTPAdapter(max_retries=retries)
)

# Connect to Athena
cursor = connect(
    s3_staging_dir="s3://ccao-athena-results-us-east-1/",
    region_name="us-east-1",
    cursor_class=PandasCursor,
).cursor(unload=True)


def parse_assets(assets: str | None = None) -> dict[str, dict[str, Any]]:
    """
    Retrieve metadata for all Socrata assets and filter that data based
    on parsed input asset names.

    Args:
        assets: Comma-separated string of asset label names to filter to.
            If None or empty, metadata for all active assets is returned.

    Returns:
        A dict keyed by asset label, where each value is a dict containing
        the keys ``asset_id`` (Socrata dataset ID), ``years_active`` (number
        of years that should be kept current), and ``athena_asset`` (fully
        qualified Athena view name).
    """

    if assets == "":
        assets = None

    # When running locally, we will probably be inside the socrata/ dir, so
    # switch back out to find the dbt/ dir
    if not os.path.isdir("./dbt"):
        os.chdir("..")

    os.chdir("./dbt")

    # If the upload is running on a semi-monthly schedule, we want to exclude
    # any assets tagged with "tag:monthly" since those are only meant to be
    # updated once per month. If the upload is running on a monthly schedule,
    # we want to include those assets.
    monthly_tag = (
        "tag:monthly" if os.getenv("UPLOAD_SCHEDULE") == "semimonthly" else ""
    )

    DBT = dbtRunner()
    dbt_list_args = [
        "--quiet",
        "list",
        "--select",
        "open_data.*",
        "--resource-types",
        "exposure",
        "--exclude",
        " ".join(filter(None, ["tag:inactive", monthly_tag])),
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

    all_assets = all_assets[
        ["label", "meta.asset_id", "meta.years_active", "athena_asset"]
    ].rename(
        columns={
            "meta.asset_id": "asset_id",
            "meta.years_active": "years_active",
        }
    )

    # If no assets are entered return metadata for all assets otherwise filter
    # by provided assets
    if assets is not None:
        assets_list = [asset.strip() for asset in str(assets).split(",")]

        all_assets = all_assets[all_assets["label"].isin(assets_list)]

    print("Assets that will be updated:")
    print("\n".join(all_assets["label"]))

    # Return a dict with labels as keys
    all_assets = all_assets.set_index("label").to_dict("index")

    return all_assets


def parse_years(years: str | None = None) -> list[str] | None:
    """
    Parse and validate the years environment variable.

    Args:
        years: A comma-separated string of four-digit year values, the
            special string ``"all"`` to select every available year, or
            None/empty string to indicate no year filter (use the default
            scheduled-run behaviour instead).

    Returns:
        A list of year strings (e.g. ``["2022", "2023"]``), the single-
        element list ``["all"]``, or None if no years were provided.
    """

    if years == "":
        years_parsed = None
    if years is not None:
        if years == "all":
            years_parsed = ["all"]
        else:
            # Only allow anticipated values
            years_parsed = [
                re.sub("[^0-9]", "", year)
                for year in str(years).split(",")
                if re.sub("[^0-9]", "", year)
            ]

    return years_parsed


def parse_years_list(
    athena_asset: str,
    years: list[str] | None = None,
    years_active: int = 1,
) -> list[Any] | None:
    """
    Determine the list of years to iterate over for an upload.

    When ``years`` is ``["all"]``, every distinct year present in the Athena
    asset is returned. When the workflow is triggered on a schedule and no
    explicit years are provided, only the most recent year(s) up to
    ``years_active`` are returned. Otherwise returns None, which signals that
    the data should be queried without a year filter.

    Args:
        athena_asset: Fully qualified Athena view name (e.g.
            ``"open_data.view_name"``). Used to query distinct or max year values
            when needed.
        years: Output of :func:`parse_years` — a list of year strings,
            ``["all"]``, or None.
        years_active: Number of consecutive recent years that should be kept
            current for this asset. Defaults to 1.

    Returns:
        A list of year values to query, or None if the asset should be
        queried without a year filter.
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
        # take whatever the max year in the asset is up till the current year.
        years_list = (
            cursor.execute(
                "SELECT MAX(year) AS year FROM "
                + athena_asset
                + " WHERE year <= "
                + str(datetime.now().year)
            )
            .as_pandas()["year"]
            .to_list()
        )

        # For assets with more than 1 active year, we want to update all active
        # years. Append years_active - 1 previous years to the list of years to
        # update. For example, if the max year in an asset is 2024 and
        # years_active is 3, we want to update 2024, 2023, and 2022 since all
        # three of those years are active.
        if years_active > 1:
            base_year = years_list[0]
            years_list.extend(
                base_year - offset for offset in range(1, years_active)
            )

    else:
        years_list = None

    return years_list


def check_overwrite(overwrite: str | bool | None = None) -> bool:
    """
    Normalize the overwrite environment variable to a Python bool.

    GitHub Actions passes all workflow inputs as strings, so this function
    handles string-to-bool coercion in addition to None/empty values.

    Args:
        overwrite: The raw value of the ``OVERWRITE`` environment variable.
            Accepts ``True``/``False`` booleans, the strings ``"true"`` or
            ``"false"``, or None/empty string (treated as False).

    Returns:
        True if the upload should fully overwrite the existing Socrata
        dataset (HTTP PUT); False if it should upsert rows instead
        (HTTP POST).
    """

    if not overwrite or overwrite == "":
        overwrite = False

    # Github inputs are passed as strings rather than booleans
    if isinstance(overwrite, str):
        overwrite = overwrite == "true"

    return overwrite


def build_query_dict(
    athena_asset: str,
    asset_id: str,
    years: list[Any] | None = None,
) -> dict[Any, str]:
    """
    Build a mapping of year keys to Athena SQL queries for a given asset.

    Compares column names between the Athena view and the live Socrata dataset
    to detect schema drift. Columns present in Athena but missing on Socrata
    generate a warning; columns missing in Athena but present on Socrata raise
    an exception (since those columns would be left unpopulated). The final
    query selects only the columns shared by both sides.

    Args:
        athena_asset: Fully qualified Athena view name (e.g.
            ``"open_data.view_name"``).
        asset_id: Socrata dataset ID (e.g. ``"wxyz-1234"``).
        years: List of year values to produce individual per-year queries for,
            or None to produce a single unfiltered query.

    Returns:
        A dict mapping year values (or None for an unfiltered query) to SQL
        query strings. NaN year values produce a ``"nan"`` key whose query
        filters on ``year IS NULL``.
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
        query_dict = {
            year: f"{query} WHERE year = {year}"
            for year in years
            if not pd.isna(year)
        }

        if any(pd.isna(years)):
            query_dict.update({"nan": f"{query} WHERE year IS NULL"})

    return query_dict


def check_deleted(input_data: pd.DataFrame, asset_id: str) -> pd.DataFrame:
    """
    Flag rows that exist on Socrata but have been removed from Athena.

    Fetches all ``row_id`` values currently stored in the Socrata asset for
    the years present in ``input_data``, then outer-joins them against the
    incoming data. Any row that exists on Socrata but is absent from
    ``input_data`` has its ``:deleted`` column set to True so that Socrata
    will remove it during the next upsert.

    Args:
        input_data: DataFrame of rows retrieved from Athena for the current
            upload batch. Must contain ``row_id`` and ``year`` columns.
        asset_id: Socrata dataset ID (e.g. ``"wxyz-1234"``).

    Returns:
        The input DataFrame with a ``:deleted`` column added. Rows that
        should be removed from Socrata have ``:deleted`` set to True; all
        other rows have it set to None.
    """

    # Determine which years are present in the input data. We only want to
    # retrieve row_ids for the corresponding years from Socrata.
    years_list = [str(year) for year in input_data["year"].unique().tolist()]
    # Unfortunately we can have null values for year, so we need to make sure
    # they are handled properly rather than being included in the IN clause
    # below
    where_list = []
    if "nan" in years_list:
        years_list.remove("nan")
        where_list.append("year is NULL")
    if any(year != "nan" for year in years_list):
        years_str = ", ".join(years_list)
        where_list.append(f"year IN ({years_str})")
    where_str = " or ".join(where_list)

    # Construct the API call to retrieve row_ids for the specified asset and years
    url = (
        f"https://datacatalog.cookcountyil.gov/resource/{asset_id}.json?$query="
        + quote(f"SELECT row_id WHERE {where_str} LIMIT 20000000")
    )

    # Retrieve row_ids from Socrata for the specified asset and years
    socrata_rows = pd.DataFrame(session.get(url=url).json())

    # Outer-join the input data with Socrata data (if it exists) to find rows
    # that are present in Socrata but not in the Athena input data. For those
    # rows set the ":deleted" column to True.
    input_data[":deleted"] = None
    if len(socrata_rows) > 0:
        input_data = input_data.merge(
            socrata_rows, on="row_id", how="outer", indicator=True
        )
        input_data.loc[input_data["_merge"] == "right_only", ":deleted"] = True
        input_data = input_data.drop(columns=["_merge"])

    return input_data


def check_missing_years(athena_asset: str, asset_id: str) -> pd.DataFrame:
    """
    Retrieve row IDs for years that exist on Socrata but have been dropped
    from the Athena view.

    This handles the case where an entire year's worth of data is removed
    from an Athena view. Because :func:`check_deleted` only inspects years
    present in the current upload batch, it would not catch stale years that
    are no longer represented in Athena at all. The returned DataFrame is
    appended to the final upload payload so those rows are deleted from
    Socrata.

    Args:
        athena_asset: Fully qualified Athena view name (e.g.
            ``"open_data.view_name"``).
        asset_id: Socrata dataset ID (e.g. ``"wxyz-1234"``).

    Returns:
        A DataFrame containing ``row_id`` and ``:deleted`` (always True)
        for every row in Socrata whose year is absent from the Athena view.
        Returns an empty DataFrame if no such rows exist.
    """

    # Grab a list of *all* years currently present in Athena asset
    athena_years = (
        cursor.execute(
            "SELECT DISTINCT CAST(year AS varchar) as year FROM "
            + athena_asset
            + " ORDER BY year"
        )
        .as_pandas()["year"]
        .to_list()
    )

    # Construct the API call to retrieve all distinct years from Socrata
    url = (
        f"https://datacatalog.cookcountyil.gov/resource/{asset_id}.json?$query="
        + quote("SELECT distinct year")
    )

    socrata_years = session.get(url=url).json()
    socrata_years = pd.DataFrame(socrata_years)

    # Socrata may return years as strings of floats (e.g., "2020.0"), so we
    # need to convert them to integers first before comparing to Athena years.
    # We also need to handle null years properly.
    socrata_years["year"] = pd.to_numeric(
        socrata_years["year"], errors="coerce"
    )
    socrata_years["year"] = (
        socrata_years["year"]
        .astype("Int64")
        .astype("str")
        .replace({"<NA>": None})
    )
    socrata_years = socrata_years["year"].tolist()

    # Determine which years are present on Socrata but not in Athena
    missing_years = set(socrata_years) - set(athena_years)

    # Note if there are null years present on Socrata so they can be handled,
    # then remove them from the missing_years set
    socrata_nulls = ""
    if None in missing_years:
        socrata_nulls = "year is NULL"
    missing_years = {item for item in missing_years if item is not None}

    # If there are any missing years, retrieve their row_ids so they can be marked
    # for deletion
    if missing_years or socrata_nulls:
        if missing_years:
            missing_years_str = ", ".join(list(missing_years))
            where = f"year in ({missing_years_str})"
        if socrata_nulls:
            where = f"{socrata_nulls}"
        if missing_years and socrata_nulls:
            where = f"year in ({missing_years_str}) or {socrata_nulls}"

        url = (
            f"https://datacatalog.cookcountyil.gov/resource/{asset_id}.json?$query="
            + quote(f"SELECT row_id WHERE {where} LIMIT 20000000")
        )
        years_to_remove = session.get(url=url).json()
        years_to_remove = pd.DataFrame(years_to_remove)
        years_to_remove[":deleted"] = True

    else:
        years_to_remove = pd.DataFrame()

    return years_to_remove


def upload(
    asset_id: str,
    sql_query: dict[Any, str],
    overwrite: bool,
    missing_years: pd.DataFrame,
) -> None:
    """
    Execute Athena queries and upload the results to a target Socrata dataset.

    Iterates over each query in ``sql_query``, fetches the corresponding data
    from Athena, optionally checks for deleted rows, and sends batched
    requests to the Socrata API in chunks of 10,000 rows. Uses HTTP PUT for
    the first batch when ``overwrite`` is True (replacing all existing rows),
    then switches to HTTP POST (upsert) for all subsequent batches.

    Args:
        asset_id: Socrata dataset ID (e.g. ``"wxyz-1234"``).
        sql_query: Mapping of year keys to SQL query strings, as produced by
            :func:`build_query_dict`.
        overwrite: If True, the first request will use HTTP PUT to replace
            existing data; subsequent requests always use HTTP POST.
        missing_years: DataFrame of row IDs (with ``:deleted`` set to True)
            for years that no longer exist in Athena, as produced by
            :func:`check_missing_years`. Appended to the final upload batch.

    Returns:
        None
    """

    url = "https://datacatalog.cookcountyil.gov/resource/" + asset_id + ".json"

    # Raise URL status if it's bad
    session.get(url=url).raise_for_status()

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
        if not overwrite:
            input_data = check_deleted(input_data, asset_id)
            # If there are years present on Socrata that are not in the current
            # upload, add them to the final input data so they can be deleted
            if year == list(sql_query.keys())[-1]:
                input_data = pd.concat(
                    [input_data, missing_years], ignore_index=True
                )

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
            )
            overwrite = False
            formatted_response = re.sub(
                '\n|{|}|"', "", response.content.decode("utf-8")
            ).strip()
            print(formatted_response)

        overwrite = False


def socrata_upload(
    asset_info: dict[str, Any],
    overwrite: bool = False,
    years: list[str] | None = None,
) -> None:
    """
    Orchestrate a full upload cycle for a single Socrata asset.

    Coordinates :func:`check_missing_years`, :func:`parse_years_list`,
    :func:`build_query_dict`, and :func:`upload` to fetch data from Athena
    and push it to the Socrata open data portal. By default, rows are upserted
    (HTTP POST) and only the most recent active year(s) are updated when
    running on a schedule.

    Args:
        asset_info: Dict containing ``athena_asset`` (Athena view name),
            ``asset_id`` (Socrata dataset ID), and ``years_active`` (number
            of recent years to keep current). Typically a value from the dict
            returned by :func:`parse_assets`.
        overwrite: If True, the existing Socrata dataset is fully replaced
            (HTTP PUT) rather than upserted. Defaults to False.
        years: List of year strings to upload, ``["all"]`` for every
            available year, or None to use the scheduled-run default
            (most recent ``years_active`` years). Defaults to None.

    Returns:
        None
    """

    athena_asset, asset_id, years_active = (
        asset_info["athena_asset"],
        asset_info["asset_id"],
        asset_info["years_active"],
    )

    missing_years = check_missing_years(athena_asset, asset_id)

    years_list = parse_years_list(
        years=years, athena_asset=athena_asset, years_active=years_active
    )

    sql_query = build_query_dict(
        athena_asset=athena_asset,
        asset_id=asset_id,
        years=years_list,
    )

    tic = time.perf_counter()
    upload(asset_id, sql_query, overwrite, missing_years)
    toc = time.perf_counter()

    print(f"Total upload in {toc - tic:0.4f} seconds")


if __name__ == "__main__":
    event_type = " ".join(
        filter(
            None,
            [os.getenv("UPLOAD_SCHEDULE"), os.getenv("WORKFLOW_EVENT_NAME")],
        )
    )
    print(f"Running upload for event type: {event_type}")

    # Retrieve asset(s)
    all_assets = parse_assets(os.getenv("SOCRATA_ASSET"))

    for asset_info in all_assets.values():
        socrata_upload(
            asset_info=asset_info,
            overwrite=check_overwrite(os.getenv("OVERWRITE")),
            years=parse_years(os.getenv("YEARS")),
        )
