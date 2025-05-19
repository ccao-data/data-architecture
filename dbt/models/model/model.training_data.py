import functools

from pyspark.sql.functions import lit


def model(dbt, session):
    # configure incremental insert-overwrite by run_id
    dbt.config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partitions_by=[{"field": "run_id", "data_type": "string"}],
        on_schema_change="append_new_columns",
    )

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

    if metadata.shape[0] == 0:
        raise ValueError(
            "No final model run data found; cannot extract training data"
        )

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
        df = session.read.parquet(f"s3://{s3_path}")

        # coerce booleans if needed
        if "ccao_is_active_exe_homeowner" in df.columns:
            df = df.withColumn(
                "ccao_is_active_exe_homeowner",
                df["ccao_is_active_exe_homeowner"].cast("boolean"),
            )
        # add run_id column
        df = df.withColumn("run_id", lit(run_id))

        all_dfs.append(df)
        print(f"Processed run_id={run_id}, rows={df.count()}")

    return functools.reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True), all_dfs
    )
