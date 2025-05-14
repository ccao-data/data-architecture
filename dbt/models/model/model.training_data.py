import re

import pandas as pd
import pyarrow.dataset as ds


def model(dbt, session):
    # overwrite only the run_id partitions we emit
    dbt.config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partitions_by=[{"field": "run_id", "data_type": "string"}],
        file_format="parquet",
        on_schema_change="append_new_columns",
    )

    # grab all final runs via Spark SQL
    query = """
        SELECT
          run_id,
          year,
          dvc_md5_assessment_data,
          model_predictor_all_name
        FROM model.metadata
        WHERE run_type = 'final'
    """
    metadata = session.sql(query).toPandas()

    all_dfs = []
    predictors_union = set()

    for _, row in metadata.iterrows():
        run_id = row["run_id"]
        year = row["year"]
        dvc_hash = row["dvc_md5_assessment_data"]
        preds_raw = row["model_predictor_all_name"]

        # parse predictor list
        preds = [
            col.strip()
            for col in re.sub(r"^\[|\]$", "", preds_raw).split(",")
            if col.strip()
        ]
        predictors_union.update(preds)

        # build the S3 path
        prefix = "" if int(year) <= 2023 else "files/md5/"
        path = f"s3://ccao-data-dvc-us-east-1/{prefix}{dvc_hash[:2]}/{dvc_hash[2:]}"

        try:
            dataset = ds.dataset(path, format="parquet")
            df = dataset.to_table(
                columns=["meta_pin", "meta_card_num"] + preds
            ).to_pandas()
        except Exception as e:
            dbt.log(f"Error reading {path}: {e}")
            continue

        # ensure correct boolean type
        if "ccao_is_active_exe_homeowner" in df.columns:
            df["ccao_is_active_exe_homeowner"] = df[
                "ccao_is_active_exe_homeowner"
            ].astype(bool)

        df["run_id"] = run_id
        df["year"] = year

        all_dfs.append(df)
        dbt.log(f"Processed run_id={run_id}, year={year}, rows={len(df)}")

    # if nothing was read, return an empty DF with the expected schema
    if not all_dfs:
        cols = ["meta_pin", "meta_card_num", "run_id", "year"] + sorted(
            predictors_union
        )
        dbt.log(
            f"No data found for any run; returning empty DataFrame with columns: {cols}"
        )
        return pd.DataFrame(columns=cols)

    # concatenate and return
    return pd.concat(all_dfs, ignore_index=True)
