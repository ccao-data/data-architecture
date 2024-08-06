import requests
import os
from pyathena import connect
from pyathena.pandas.util import as_pandas
from pyathena.pandas.cursor import PandasCursor
from dotenv import load_dotenv

load_dotenv(".Renviron")
cursor = connect(
    s3_staging_dir=os.getenv("AWS_ATHENA_S3_STAGING_DIR") + "/",
    region_name=os.getenv("AWS_REGION"),
    cursor_class=PandasCursor,
).cursor(unload=True)
app_token = os.getenv("SOCRATA_APP_TOKEN")
auth = (os.getenv("SOCRATA_USERNAME"), os.getenv("SOCRATA_PASSWORD"))


def build_query(athena_asset, row_identifiers=["pin", "year"], by_year=False):
    row_identifier = "CONCAT(" + ", ".join(row_identifiers) + ") AS row_id,"

    columns = as_pandas(
        cursor.execute(
            "show columns from " + athena_asset,
        )
    )

    columns.loc[columns["type"] == "array(varchar)", "column"] = (
        "ARRAY_JOIN("
        + columns[columns["type"] == "array(varchar)"]["column"]
        + ", ', ') AS "
        + columns[columns["type"] == "array(varchar)"]["column"]
    )

    if not by_year:
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
    else:
        raise TypeError("Only methods 'put' or 'post' allowed.")

    return response


def socrata_upload(
    asset_id,
    athena_asset,
    row_identifiers=["pin", "year"],
    by_year=False,
    overwrite=False,
    years=None,
):
    sql_query = build_query(
        athena_asset=athena_asset,
        row_identifiers=row_identifiers,
        by_year=by_year,
    )

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
    asset_id="4u8x-wdnz",
    athena_asset="default.vw_pin_universe",
    row_identifiers=["pin", "year"],
    by_year=False,
    overwrite=False,
    years=None,
)
