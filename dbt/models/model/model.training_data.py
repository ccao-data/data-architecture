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

    if dbt.is_incremental:
        # Get max year already in the table
        existing_max_year_df = dbt.ref("model_training_data").to_pandas()
        max_loaded_year = existing_max_year_df["year"].max()
    else:
        max_loaded_year = 0

    # Query final model metadata for years > max_loaded_year
    query = f"""
        SELECT run_id, year, dvc_md5_assessment_data, model_predictor_all_name
        FROM model.metadata
        WHERE run_type = 'final'
          AND year > {max_loaded_year}
    """
    metadata = session.execute(query).fetch_arrow_table().to_pandas()

    if metadata.empty:
        return pd.DataFrame()  # nothing new to process

    all_dfs = []

    for _, row in metadata.iterrows():
        run_id = row["run_id"]
        year = row["year"]
        dvc_hash = row["dvc_md5_assessment_data"]
        predictors_raw = row["model_predictor_all_name"]

        predictors = [
            col.strip()
            for col in re.sub(r"^\[|\]$", "", predictors_raw).split(",")
        ]

        # Construct DVC path depending on year
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

        # Ensure type consistency for a column with different types
        if "ccao_is_active_exe_homeowner" in df.columns:
            df["ccao_is_active_exe_homeowner"] = df[
                "ccao_is_active_exe_homeowner"
            ].astype(bool)

        df["run_id"] = run_id
        df["year"] = year

        all_dfs.append(df)

        dbt.log(f"Processed run_id={run_id}, year={year}, rows={len(df)}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()
