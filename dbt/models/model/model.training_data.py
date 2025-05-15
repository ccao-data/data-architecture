import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as fs


def model(dbt, session):
    # configure incremental insert-overwrite by run_id
    dbt.config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partitions_by=[{"field": "run_id", "data_type": "string"}],
        file_format="parquet",
        on_schema_change="append_new_columns",
    )

    # build a PyArrow S3FileSystem with the right region
    s3 = fs.S3FileSystem(region="us-east-1")

    # grab all final runs via Spark SQL
    query = """
        SELECT
          CAST(run_id                  AS STRING) AS run_id,
          CAST(year                    AS STRING) AS year,
          CAST(dvc_md5_assessment_data AS STRING) AS dvc_md5_assessment_data
        FROM model.metadata
        WHERE run_type = 'final'
    """
    metadata = session.sql(query).toPandas()

    bucket = "ccao-data-dvc-us-east-1"
    all_dfs = []

    for _, row in metadata.iterrows():
        run_id = row["run_id"]
        year = int(row["year"])
        dvc_hash = row["dvc_md5_assessment_data"]

        # for post-2023 we have the extra files/md5/ prefix
        prefix = "" if year <= 2023 else "files/md5/"
        key = f"{prefix}{dvc_hash[:2]}/{dvc_hash[2:]}"
        s3_path = f"{bucket}/{key}"

        print(f">>> reading all columns for run {run_id!r}")
        print(f"    →   S3 key = {s3_path}")

        try:
            # note: no "s3://", just "bucket/key/..."
            dataset = ds.dataset(s3_path, filesystem=s3, format="parquet")
            df = dataset.to_table().to_pandas()
        except Exception as e:
            print(f"[WARN] Error reading {s3_path!r}: {e}")
            continue

        # coerce booleans if needed
        if "ccao_is_active_exe_homeowner" in df:
            df["ccao_is_active_exe_homeowner"] = df[
                "ccao_is_active_exe_homeowner"
            ].astype(bool)

        df["run_id"] = run_id
        all_dfs.append(df)
        print(f"[INFO] Processed run_id={run_id}, rows={len(df)}")

    if not all_dfs:
        print("[INFO] No data found; returning empty DataFrame.")
        return pd.DataFrame(columns=["run_id"])

    return pd.concat(all_dfs, ignore_index=True)
