import requests
import os
from pyathena import connect
from pyathena.pandas.util import as_pandas
from pyathena.pandas.cursor import PandasCursor
from dotenv import load_dotenv

# Load environmental variables and connect to Athena
load_dotenv(".Renviron")
app_token = os.getenv("SOCRATA_APP_TOKEN")
auth = (os.getenv("SOCRATA_USERNAME"), os.getenv("SOCRATA_PASSWORD"))
cursor = connect(
    s3_staging_dir=os.getenv("AWS_ATHENA_S3_STAGING_DIR") + "/",
    region_name=os.getenv("AWS_REGION"),
    cursor_class=PandasCursor,
).cursor(unload=True)

# Define open data assets and related information
tables = {
    "Parcel Universe": {
        "athena_asset": "default.vw_pin_universe",
        "asset_id": "4u8x-wdnz",
        "primary_key": ["pin", "year"],
    },
    "Single and Multi-Family Improvement Characteristics": {
        "athena_asset": "default.vw_card_res_char",
        "asset_id": "x54s-btds",
        "primary_key": ["pin", "card", "year"],
    },
    "Residential Condominium Unit Characteristics": {
        "athena_asset": "default.vw_pin_condo_char",
        "asset_id": "3r7i-mrz4",
        "primary_key": ["pin", "year"],
    },
    "Parcel Sales": {
        "athena_asset": "default.vw_pin_sale",
        "asset_id": "wvhk-k5uv",
        "primary_key": ["doc_no"],
    },
    "Assessed Values": {
        "athena_asset": "default.vw_pin_history",
        "asset_id": "uzyt-m557",
        "primary_key": ["pin", "year"],
    },
    "Appeals": {
        "athena_asset": "default.vw_pin_appeal",
        "asset_id": "y282-6ig3",
        "primary_key": ["pin", "year"],
    },
    "Parcel Addresses": {
        "athena_asset": "default.vw_pin_address",
        "asset_id": "3723-97qp",
        "primary_key": ["pin", "year"],
    },
    "Parcel Proximity": {
        "athena_asset": "proximity.vw_pin10_proximity",
        "asset_id": "ydue-e5u3",
        "primary_key": ["pin", "year"],
    },
    "Property Tax-Exempt Parcels": {
        "athena_asset": "default.vw_pin_exempt",
        "asset_id": "vgzx-68gb",
        "primary_key": ["pin10", "year"],
    },
}


def get_asset_info(socrata_asset):
    """
    Simple helper function to retrieve asset-specific information needed for
    other functions.
    """

    athena_asset = tables.get(socrata_asset).get("athena_asset")
    asset_id = tables.get(socrata_asset).get("asset_id")
    row_identifier = tables.get(socrata_asset).get("primary_key")
    return athena_asset, asset_id, row_identifier


def build_query(athena_asset, row_identifier, years=None):
    """
    Build an Athena compatible SQL query. Function will append a year
    conditional if `years` is non-empty. Many of the CCAO's open data assets are
    too large to pass to Socrata without chunking. A `row_id` column is
    constructed in order to use Socrata's `upsert` functionalities (updating
    rather than overwriting data that already exists) based on the column names
    passed to `row_indetifiers`.
    """

    row_identifier = "CONCAT(" + ", ".join(row_identifier) + ") AS row_id,"

    # Retrieve column names and types from Athena
    columns = as_pandas(
        cursor.execute(
            "show columns from " + athena_asset,
        )
    )

    # Array type columns are not compatible with the json format needed for
    # Socrata uploads. Automatically convert any array type columns to string.
    columns.loc[columns["type"] == "array(varchar)", "column"] = (
        "ARRAY_JOIN("
        + columns[columns["type"] == "array(varchar)"]["column"]
        + ", ', ') AS "
        + columns[columns["type"] == "array(varchar)"]["column"]
    )

    if not years:
        query = (
            "SELECT\n"
            + row_identifier
            + "\n"
            + ",\n".join(columns["column"])
            + "\nFROM "
            + athena_asset
            + "\nLIMIT 1000"
        )

    else:
        query = (
            "SELECT\n"
            + row_identifier
            + "\n"
            + ",\n".join(columns["column"])
            + "\nFROM "
            + athena_asset
            + "\nWHERE year = %(year)s"
            + "\nLIMIT 1000"
        )

    return query


def upload(method, asset_id, sql_query, year=None):
    """
    Function to perform the upload to Socrata. `puts` or `posts` depending on
    user's choice to overwrite existing data.
    """

    url = (
        "https://datacatalog.cookcountyil.gov/resource/"
        + asset_id
        + ".json?$$app_token="
        + app_token
    )

    if method == "put":
        print(
            "Overwriting all years for asset", asset_id
        ) if not year else print("Overwriting", year, "for asset", asset_id)
        response = requests.put(
            url=url,
            data=as_pandas(cursor.execute(sql_query, {"year": year})).to_json(
                orient="records"
            ),
            auth=auth,
        )
    elif method == "post":
        print("Updating all years for asset", asset_id) if not year else print(
            "Updating", year, "for asset", asset_id
        )
        response = requests.post(
            url=url,
            data=as_pandas(cursor.execute(sql_query, {"year": year})).to_json(
                orient="records"
            ),
            auth=auth,
        )

    return response


def socrata_upload(
    socrata_asset,
    overwrite=False,
    years="all",
):
    """
    Wrapper function for building SQL query, retrieving data from Athena, and
    uploading it to Socrata. Allows users to specify target Athena and Socrata
    assets, define columns to construct a `row_id`, whether the data on Socrata
    should be overwritten or updated, and whether or not to chunk the upload by
    year. By default the function will query a given Athena asset by year for
    all years and upload via `post` (update rather than overwrite).
    """

    athena_asset, asset_id, row_identifier = get_asset_info(socrata_asset)

    sql_query = build_query(
        athena_asset=athena_asset,
        row_identifier=row_identifier,
        years=years,
    )

    if years == "all":
        years = as_pandas(
            cursor.execute(
                "SELECT DISTINCT year FROM " + athena_asset + " ORDER BY year"
            )
        )["year"].to_list()

    count = 0

    if not years:
        if overwrite:
            response = upload("put", asset_id=asset_id, sql_query=sql_query)
        else:
            response = upload("post", asset_id=asset_id, sql_query=sql_query)
        print(response.content)

    else:
        if overwrite:
            for item in years:
                if count == 0:
                    response = upload(
                        "put",
                        asset_id=asset_id,
                        sql_query=sql_query,
                        year=item,
                    )
                else:
                    response = upload(
                        "post",
                        asset_id=asset_id,
                        sql_query=sql_query,
                        year=item,
                    )
                print(response.content)
                count = count + 1

        else:
            for item in years:
                response = upload(
                    "post", asset_id=asset_id, sql_query=sql_query, year=item
                )
                print(response.content)


socrata_upload(
    socrata_asset="Parcel Universe",
    overwrite=True,
    years="all",
)
