import re

import pandas as pd
import pyarrow.dataset as ds

# dbt configs
config = {
    "materialized": "incremental",
    "file_format": "parquet",
    "incremental_strategy": "insert_overwrite",
    "on_schema_change": "append_new_columns",
}


def model(dbt, session):
    dbt.config(**config)

    # Get the maximum year
    if dbt.is_incremental:
        existing = dbt.ref("model.training_data").to_pandas()
        max_loaded_year = existing["year"].max() if not existing.empty else 0
    else:
        max_loaded_year = 0

    # Query metadata for new runs
    query = f"""
        SELECT run_id, year, dvc_md5_assessment_data, model_predictor_all_name
        FROM model.metadata
        WHERE run_type = 'final'
          AND year > {max_loaded_year}
    """
    metadata = session.execute(query).fetch_arrow_table().to_pandas()

    if metadata.empty:
        dbt.log("No new metadata found.")
        return pd.DataFrame(
            columns=["meta_pin", "meta_card_num", "run_id", "year"]
        )

    all_dfs = []
    predictors_union = set()

    for _, row in metadata.iterrows():
        run_id = row["run_id"]
        year = row["year"]
        dvc_hash = row["dvc_md5_assessment_data"]
        predictors_raw = row["model_predictor_all_name"]

        predictors = [
            col.strip()
            for col in re.sub(r"^\[|\]$", "", predictors_raw).split(",")
        ]
        predictors_union.update(predictors)

        dvc_prefix = "" if int(year) <= 2023 else "files/md5/"
        dvc_path = f"s3://ccao-data-dvc-us-east-1/{dvc_prefix}{dvc_hash[:2]}/{dvc_hash[2:]}"

        try:
            dataset = ds.dataset(dvc_path, format="parquet")
            df = dataset.to_table(
                columns=["meta_pin", "meta_card_num"] + predictors
            ).to_pandas()
        except Exception as e:
            dbt.log(f"Error reading {dvc_path}: {e}")
            continue

        if "ccao_is_active_exe_homeowner" in df.columns:
            df["ccao_is_active_exe_homeowner"] = df[
                "ccao_is_active_exe_homeowner"
            ].astype(bool)

        df["run_id"] = run_id
        df["year"] = year

        all_dfs.append(df)
        dbt.log(f"Processed run_id={run_id}, year={year}, rows={len(df)}")

    if not all_dfs:
        dbt.log("No new records to process.")
        return pd.DataFrame(
            columns=["meta_pin", "meta_card_num", "run_id", "year"]
            + sorted(predictors_union)
        )

    result_df = pd.concat(all_dfs, ignore_index=True)
    return result_df
